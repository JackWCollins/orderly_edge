# ---
# jupyter:
#   jupytext:
#     formats: py:percent
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.16.4
#   kernelspec:
#     display_name: Python 3 (ipykernel)
#     language: python
#     name: python3
# ---

# %%
from decimal import Decimal
import datetime
from typing import Dict, List, Optional, Tuple
import numpy as np

from nautilus_trader.config import PositiveFloat
from nautilus_trader.config import PositiveInt
from nautilus_trader.config import StrategyConfig
from nautilus_trader.core.rust.common import LogColor
from nautilus_trader.model.book import OrderBook
from nautilus_trader.model.data import OrderBookDeltas
from nautilus_trader.model.data import TradeTick
from nautilus_trader.model.data import BarType
from nautilus_trader.model.data import Bar
from nautilus_trader.model.enums import BookType
from nautilus_trader.model.enums import OrderSide
from nautilus_trader.model.enums import TimeInForce
from nautilus_trader.model.enums import book_type_from_str
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.instruments import Instrument
from nautilus_trader.model.objects import Price
from nautilus_trader.model.objects import Quantity
from nautilus_trader.trading.strategy import Strategy


class EnhancedAbsorptionConfig(StrategyConfig, frozen=True):
    """
    Configuration for ``EnhancedAbsorptionStrategy`` instances.

    Parameters
    ----------
    instrument_id : InstrumentId
        The instrument ID for the strategy.
    min_absorption_volume : PositiveFloat
        Minimum volume required to determine absorption is occurring.
    liquidity_threshold : PositiveFloat
        Threshold to determine areas of high liquidity (min volume at a level).
    monitor_levels : PositiveInt, default 5
        Number of price levels to monitor for absorption.
    cooldown_period_seconds : PositiveFloat, default 10.0
        Cooldown period in seconds between trade entries.
    trade_size : Decimal, default 1.0
        Fixed size for trades (e.g., 1 contract).
    """

    instrument_id: InstrumentId
    min_absorption_volume: PositiveFloat
    liquidity_threshold: PositiveFloat
    monitor_levels: PositiveInt = 5
    cooldown_period_seconds: PositiveFloat = 10.0
    trade_size: Decimal = Decimal("1.0")


class EnhancedAbsorptionStrategy(Strategy):
    """
    A strategy that monitors for large volume being traded into areas of high liquidity,
    and takes small reversal trades when specific absorption patterns are detected.
    
    This strategy:
    1. Identifies areas of high liquidity in the order book
    2. Detects when significant volume is being traded into these areas
    3. Takes contrarian trades when absorption thresholds are met
    4. Uses a fixed trade size for simplicity and risk management

    Parameters
    ----------
    config : EnhancedAbsorptionConfig
        The configuration for the instance.
    """

    def __init__(self, config: EnhancedAbsorptionConfig) -> None:
        super().__init__(config)

        # Initialized in on_start
        self.instrument: Optional[Instrument] = None
        self.book_type: BookType = book_type_from_str("L3_MBO")
        
        # For tracking order book state
        self._prev_bid_levels: Dict[Price, Quantity] = {}
        self._prev_ask_levels: Dict[Price, Quantity] = {}
        self._last_trade_timestamp: Optional[datetime.datetime] = None
        
        # For identifying high liquidity areas
        self._bid_liquidity_areas: List[Price] = []
        self._ask_liquidity_areas: List[Price] = []
        
        # For tracking absorption metrics
        self._absorption_events: List[Dict] = []
        self._trades_taken: int = 0
        self._profitable_trades: int = 0

        self._last_check_time = None  # Initialize last check time

    def on_start(self) -> None:
        """Actions to be performed on strategy start."""
        self.instrument = self.cache.instrument(self.config.instrument_id)
        if self.instrument is None:
            self.log.error(f"Could not find instrument for {self.config.instrument_id}")
            self.stop()
            return

        # Subscribe to order book deltas and trades
        self.subscribe_order_book_deltas(self.instrument.id, self.book_type)
        self.subscribe_trade_ticks(self.instrument.id)
        # bar_type = BarType.from_str(f"{self.instrument.id}-1-MINUTE-LAST-INTERNAL")
        # self.subscribe_bars(bar_type)
        
        # Initialize timestamp
        self._last_trade_timestamp = self.clock.utc_now()

        self.log.info(
            f"Starting Enhanced Absorption Strategy for {self.instrument.id.symbol}",
            color=LogColor.BLUE,
        )

    def on_order_book_deltas(self, deltas: OrderBookDeltas) -> None:
        """
        Actions to be performed when order book deltas are received.
        Updates the internal state, identifies liquidity areas, and checks for absorption.
        """

            # Assuming deltas is a list and we take the timestamp from the first delta
        if not deltas.deltas:
            return  # No deltas to process

        current_event_time = deltas.deltas[0].ts_event  # Get the timestamp from the first delta

        if self._last_check_time is None or (current_event_time - self._last_check_time) >= 1_000_000_000:  # 1 second in nanoseconds
            # Update high liquidity areas
            self.identify_liquidity_areas()
            
            # Check for absorption patterns
            self.check_for_absorption()

            # Update the last check time
            self._last_check_time = current_event_time

    def on_trade_tick(self, tick: TradeTick) -> None:
        """
        Actions to be performed when trade ticks are received.
        Used to track actual trades occurring in the market.
        """
        self.log.info(f"Trade: {tick.price} x {tick.size} ({tick.side})")
        
        # We could use this to enhance our absorption detection
        # by tracking the actual trades happening at each level
        pass

    # def on_bar(self, bar: Bar) -> None:
    #     """
    #     Actions to be performed when a bar is received.
    #     """
    #     self.log.info(f"Bar: {bar.timestamp} {bar.open} {bar.high} {bar.low} {bar.close} {bar.volume}")

    #     # Identify liquidity areas
    #     self.identify_liquidity_areas()

    #     # Check for absorption patterns
    #     self.check_for_absorption()


    def identify_liquidity_areas(self) -> None:
        """
        Identify areas of high liquidity in the current order book.
        These are defined as price levels with volume exceeding our liquidity threshold.
        """
        book = self.cache.order_book(self.config.instrument_id)
        if not book or not book.bids() or not book.asks():
            return  # Wait for complete book
            
        # Clear previous liquidity areas
        self._bid_liquidity_areas = []
        self._ask_liquidity_areas = []

        # Check bid side for high liquidity
        for level in book.bids()[:self.config.monitor_levels]:
            total_level_size = sum(order.size.as_double() for order in level.orders())
            if total_level_size > self.config.liquidity_threshold:
                self._bid_liquidity_areas.append(level.price)
                self.log.info(f"High bid liquidity at {level.price}: {total_level_size}")
                
        # Check ask side for high liquidity
        for level in book.asks()[:self.config.monitor_levels]:
            total_level_size = sum(order.size.as_double() for order in level.orders())
            if total_level_size > self.config.liquidity_threshold:
                self._ask_liquidity_areas.append(level.price)
                self.log.info(f"High ask liquidity at {level.price}: {total_level_size}")

    def check_for_absorption(self) -> None:
        """
        Check for absorption conditions in the order book.
        
        We define absorption as significant volume decrease in high liquidity areas,
        indicating that large orders are being executed against the resting liquidity.
        """
        if not self.instrument:
            return

        # Get the current order book from cache
        book = self.cache.order_book(self.config.instrument_id)
        if not book or not book.bids() or not book.asks():
            return  # Wait for complete book

        # Get current bid/ask levels
        current_bid_levels = self._extract_levels(book.bids()[:self.config.monitor_levels])
        current_ask_levels = self._extract_levels(book.asks()[:self.config.monitor_levels])

        # Check for absorption if we have previous levels to compare with
        if self._prev_bid_levels and self._prev_ask_levels:
            # Calculate volume changes, but focus on high liquidity areas
            bid_absorption = self._calculate_liquidity_absorption(
                self._prev_bid_levels, 
                current_bid_levels,
                self._bid_liquidity_areas
            )
            
            ask_absorption = self._calculate_liquidity_absorption(
                self._prev_ask_levels, 
                current_ask_levels,
                self._ask_liquidity_areas
            )

            # Check for significant absorption patterns and act
            self._check_and_act_on_absorption(bid_absorption, ask_absorption, book)

        # Update previous levels
        self._prev_bid_levels = current_bid_levels
        self._prev_ask_levels = current_ask_levels

    def _extract_levels(self, levels: List) -> Dict[Price, Quantity]:
        """
        Extract price and size from book levels.
        
        Parameters
        ----------
        levels : List
            List of book levels.
            
        Returns
        -------
        Dict[Price, Quantity]
            Dictionary mapping price to size.
        """
        result = {}
        for level in levels:
            total_level_size = Quantity(0, self.instrument.size_precision)
            for order in level.orders():
                total_level_size += order.size
            result[level.price] = total_level_size
        return result

    def _calculate_liquidity_absorption(
        self, 
        prev_levels: Dict[Price, Quantity], 
        current_levels: Dict[Price, Quantity],
        liquidity_areas: List[Price]
    ) -> Tuple[float, List[Price]]:
        """
        Calculate the absorption amount focusing on high liquidity areas.
        
        Parameters
        ----------
        prev_levels : Dict[Price, Quantity]
            Previous price levels.
        current_levels : Dict[Price, Quantity]
            Current price levels.
        liquidity_areas : List[Price]
            High liquidity price areas to focus on.
            
        Returns
        -------
        Tuple[float, List[Price]]
            The total absorption volume and affected price levels.
        """
        absorption_volume = 0.0
        absorbed_prices = []
        
        # Focus on high liquidity areas
        for price in liquidity_areas:
            if price in prev_levels:
                # If the price level still exists but with reduced size
                if price in current_levels and current_levels[price] < prev_levels[price]:
                    # This is likely due to trades (absorption)
                    volume_change = (float(prev_levels[price]) - float(current_levels[price]))
                    absorption_volume += volume_change
                    absorbed_prices.append(price)
                    self.log.debug(f"Absorption at {price}: {volume_change}")
                
                # If the price level is gone completely (complete absorption)
                elif price not in current_levels:
                    absorption_volume += float(prev_levels[price])
                    absorbed_prices.append(price)
                    self.log.debug(f"Complete absorption at {price}: {prev_levels[price].as_double()}")
                    
        return absorption_volume, absorbed_prices

    def _check_and_act_on_absorption(
        self, 
        bid_absorption: Tuple[float, List[Price]], 
        ask_absorption: Tuple[float, List[Price]], 
        book: OrderBook
    ) -> None:
        """
        Check if absorption meets criteria and execute trades accordingly.
        
        Parameters
        ----------
        bid_absorption : Tuple[float, List[Price]]
            Volume absorbed from the bid side and affected prices.
        ask_absorption : Tuple[float, List[Price]]
            Volume absorbed from the ask side and affected prices.
        book : OrderBook
            Current order book.
        """
        bid_volume, bid_prices = bid_absorption
        ask_volume, ask_prices = ask_absorption
        
        # Check cooldown period
        seconds_since_last_trade = (
            self.clock.utc_now() - self._last_trade_timestamp
        ).total_seconds()
        
        if seconds_since_last_trade < self.config.cooldown_period_seconds:
            return
            
        # Log absorption values for monitoring
        # self.log.info(
        #     f"Absorption values - Bid: {bid_volume:.2f} @ {bid_prices}, Ask: {ask_volume:.2f} @ {ask_prices}",
        # )
        
        # Record this absorption event
        self._absorption_events.append({
            "timestamp": self.clock.utc_now(),
            "bid_absorption": bid_volume,
            "bid_prices": [str(p) for p in bid_prices],
            "ask_absorption": ask_volume,
            "ask_prices": [str(p) for p in ask_prices],
        })
        
        # Determine if absorption threshold is met and which side is being absorbed
        if bid_volume > self.config.min_absorption_volume and bid_volume > ask_volume * 1.5:
            # Bid side is being absorbed, enter a small SELL order (reversal)
            self.log.info(
                f"ACTION: Detected significant bid absorption: {bid_volume:.2f}",
                color=LogColor.MAGENTA,
            )
            
            # Get best bid price (we'll sell at this price)
            best_bid = book.best_bid_price()
            if best_bid:
                # Create and submit the sell order with fixed size
                trade_qty = self.instrument.make_qty(Quantity(self.config.trade_size, self.instrument.size_precision))
                order = self.order_factory.limit(
                    instrument_id=self.instrument.id,
                    price=self.instrument.make_price(best_bid),
                    order_side=OrderSide.SELL,
                    quantity=trade_qty,
                    post_only=False,
                    time_in_force=TimeInForce.FOK,
                )
                
                self._last_trade_timestamp = self.clock.utc_now()
                self._trades_taken += 1
                self.log.info(f"Submitting SELL order: {order}", color=LogColor.RED)
                self.submit_order(order)
                
        elif ask_volume > self.config.min_absorption_volume and ask_volume > bid_volume * 1.5:
            # Ask side is being absorbed, enter a small BUY order (reversal)
            self.log.info(
                f"ACTION: Detected significant ask absorption: {ask_volume:.2f}",
                color=LogColor.MAGENTA,
            )
            
            # Get best ask price (we'll buy at this price)
            best_ask = book.best_ask_price()
            if best_ask:
                # Create and submit the buy order with fixed size
                trade_qty = self.instrument.make_qty(Quantity(self.config.trade_size, self.instrument.size_precision))
                order = self.order_factory.limit(
                    instrument_id=self.instrument.id,
                    price=self.instrument.make_price(best_ask),
                    order_side=OrderSide.BUY,
                    quantity=trade_qty,
                    post_only=False,
                    time_in_force=TimeInForce.FOK,
                )
                
                self._last_trade_timestamp = self.clock.utc_now()
                self._trades_taken += 1
                self.log.info(f"Submitting BUY order: {order}", color=LogColor.GREEN)
                self.submit_order(order)

    def on_stop(self) -> None:
        """Actions to be performed when the strategy is stopped."""
        # Log strategy summary
        self.log.info(f"Strategy stopped. Total trades taken: {self._trades_taken}")
        
        # Log absorption events summary if any
        if self._absorption_events:
            avg_bid_absorption = np.mean([e["bid_absorption"] for e in self._absorption_events])
            avg_ask_absorption = np.mean([e["ask_absorption"] for e in self._absorption_events])
            self.log.info(
                f"Absorption events detected: {len(self._absorption_events)}, "
                f"Avg Bid: {avg_bid_absorption:.2f}, Avg Ask: {avg_ask_absorption:.2f}"
            )