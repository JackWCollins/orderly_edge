from decimal import Decimal
import datetime
from typing import Dict, List, Optional

from nautilus_trader.config import PositiveFloat
from nautilus_trader.config import PositiveInt
from nautilus_trader.config import StrategyConfig
from nautilus_trader.core.rust.common import LogColor
from nautilus_trader.model.book import OrderBook
from nautilus_trader.model.data import OrderBookDeltas
from nautilus_trader.model.data import QuoteTick
from nautilus_trader.model.enums import BookType
from nautilus_trader.model.enums import OrderSide
from nautilus_trader.model.enums import TimeInForce
from nautilus_trader.model.enums import book_type_from_str
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.instruments import Instrument
from nautilus_trader.model.objects import Price
from nautilus_trader.model.objects import Quantity
from nautilus_trader.trading.strategy import Strategy


class AbsorptionConfig(StrategyConfig, frozen=True):
    """
    Configuration for ``AbsorptionStrategy`` instances.

    Parameters
    ----------
    instrument_id : InstrumentId
        The instrument ID for the strategy.
    trade_size : Decimal
        The size for trades to enter on absorption.
    min_absorption_volume : PositiveFloat
        Minimum volume required to determine absorption is occurring.
    monitor_levels : PositiveInt, default 3
        Number of price levels to monitor for absorption.
    cooldown_period_seconds : PositiveFloat, default 10.0
        Cooldown period in seconds between trade entries.
    trade_pct_of_absorption : PositiveFloat, default 0.1
        Size of the trade as a percentage of detected absorption volume.
    max_trade_size : PositiveFloat, default 1000.0
        Maximum trade size regardless of absorption volume.
    """

    instrument_id: InstrumentId
    trade_size: Decimal
    min_absorption_volume: PositiveFloat
    monitor_levels: PositiveInt = 3
    cooldown_period_seconds: PositiveFloat = 10.0
    trade_pct_of_absorption: PositiveFloat = 0.1
    max_trade_size: PositiveFloat = 1000.0


class AbsorptionStrategy(Strategy):
    """
    A strategy that monitors order book deltas for absorption and enters small trades
    on the opposite side.

    The strategy works by:
    1. Building and maintaining an order book using OrderBookDeltas
    2. Tracking changes in order book sizes over time
    3. Detecting when orders on one side are being "absorbed" (executed against)
    4. Entering a small trade on the opposite side of the absorption

    Parameters
    ----------
    config : AbsorptionConfig
        The configuration for the instance.
    """

    def __init__(self, config: AbsorptionConfig) -> None:
        super().__init__(config)

        # Initialized in on_start
        self.instrument: Optional[Instrument] = None
        self.book_type: BookType = book_type_from_str("L3_MBO")
        
        # For tracking order book state
        self._prev_bid_levels: Dict[Price, Quantity] = {}
        self._prev_ask_levels: Dict[Price, Quantity] = {}
        self._last_trade_timestamp: Optional[datetime.datetime] = None

        # Trade size parameter - dynamically calculated based on absorption volume
        self.base_trade_size = config.trade_size

    def on_start(self) -> None:
        """
        Actions to be performed on strategy start.
        """
        self.instrument = self.cache.instrument(self.config.instrument_id)
        if self.instrument is None:
            self.log.error(f"Could not find instrument for {self.config.instrument_id}")
            self.stop()
            return

        # Subscribe to order book deltas
        self.subscribe_order_book_deltas(self.instrument.id, self.book_type)
        
        # Initialize timestamp
        self._last_trade_timestamp = self.clock.utc_now()
        
        self.log.info(
            f"Starting Absorption Strategy for {self.instrument.id.symbol}",
            color=LogColor.BLUE,
        )

    def on_order_book_deltas(self, deltas: OrderBookDeltas) -> None:
        """
        Actions to be performed when order book deltas are received.
        Updates the internal state and checks for absorption.
        """
        # The OrderBook will be updated automatically in the cache
        self.check_for_absorption()

    def check_for_absorption(self) -> None:
        """
        Check for absorption conditions in the order book.
        
        Absorption is detected when:
        1. There is significant volume decrease on one side of the book
        2. The decrease is not due to price moves but due to trades
        """
        if not self.instrument:
            self.log.error("No instrument loaded")
            return

        # Get the current order book from cache
        book = self.cache.order_book(self.config.instrument_id)
        if not book:
            self.log.error("No order book being maintained")
            return

        if not book.bids() or not book.asks():
            return  # Wait for complete book

        # Get current bid/ask levels (limited to monitor_levels)
        current_bid_levels = self._extract_levels(book.bids()[:self.config.monitor_levels])
        current_ask_levels = self._extract_levels(book.asks()[:self.config.monitor_levels])

        # Check for absorption if we have previous levels to compare with
        if self._prev_bid_levels and self._prev_ask_levels:
            # Calculate volume changes
            bid_absorption = self._calculate_absorption(self._prev_bid_levels, current_bid_levels)
            ask_absorption = self._calculate_absorption(self._prev_ask_levels, current_ask_levels)

            # Check and act on absorption
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
            for order in level.orders():
                result[order.price] = order.size
        return result

    def _calculate_absorption(
        self, 
        prev_levels: Dict[Price, Quantity], 
        current_levels: Dict[Price, Quantity]
    ) -> float:
        """
        Calculate the absorption amount (volume decrease) between previous and current levels.
        
        Parameters
        ----------
        prev_levels : Dict[Price, Quantity]
            Previous price levels.
        current_levels : Dict[Price, Quantity]
            Current price levels.
            
        Returns
        -------
        float
            The total absorption volume.
        """
        absorption_volume = 0.0
        
        # Check for decreased sizes at each price level
        for price, prev_size in prev_levels.items():
            # If the price level still exists but with reduced size
            if price in current_levels and current_levels[price] < prev_size:
                # This is likely due to trades (absorption)
                absorption_volume += (prev_size - current_levels[price]).as_double()
            # If the price level is gone completely, it might be due to a complete fill
            elif price not in current_levels:
                # Check if it's not just a price movement by ensuring higher prices weren't added
                # This logic can be refined based on specific market behavior
                if not any(p > price for p in current_levels if p not in prev_levels):
                    absorption_volume += prev_size.as_double()
                    
        return absorption_volume

    def _check_and_act_on_absorption(
        self, 
        bid_absorption: float, 
        ask_absorption: float, 
        book: OrderBook
    ) -> None:
        """
        Check if absorption meets criteria and execute trades accordingly.
        
        Parameters
        ----------
        bid_absorption : float
            Volume absorbed from the bid side.
        ask_absorption : float
            Volume absorbed from the ask side.
        book : OrderBook
            Current order book.
        """
        # Check cooldown period
        seconds_since_last_trade = (
            self.clock.utc_now() - self._last_trade_timestamp
        ).total_seconds()
        
        if seconds_since_last_trade < self.config.cooldown_period_seconds:
            return
            
        # Log absorption values
        self.log.info(
            f"Absorption values - Bid: {bid_absorption:.6f}, Ask: {ask_absorption:.6f}",
        )
        
        # Determine if absorption threshold is met and which side is being absorbed
        if bid_absorption > self.config.min_absorption_volume and bid_absorption > ask_absorption * 2:
            # Bid side is being absorbed significantly, enter a small sell order
            self.log.info(
                f"Detected significant bid absorption: {bid_absorption:.6f}",
                color=LogColor.BLUE,
            )
            
            # Calculate trade size as percentage of absorption with maximum limit
            trade_size = min(
                bid_absorption * self.config.trade_pct_of_absorption,
                float(self.config.max_trade_size)
            )
            
            # Get best bid price (we'll sell at this price)
            best_bid = book.best_bid_price()
            if best_bid:
                # Create and submit the sell order
                trade_qty = self.instrument.make_qty(Quantity(trade_size, self.instrument.size_precision))
                order = self.order_factory.limit(
                    instrument_id=self.instrument.id,
                    price=self.instrument.make_price(best_bid),
                    order_side=OrderSide.SELL,
                    quantity=trade_qty,
                    post_only=False,
                    time_in_force=TimeInForce.FOK,
                )
                
                self._last_trade_timestamp = self.clock.utc_now()
                self.log.info(f"Submitting sell order: {order}", color=LogColor.BLUE)
                self.submit_order(order)
                
        elif ask_absorption > self.config.min_absorption_volume and ask_absorption > bid_absorption * 2:
            # Ask side is being absorbed significantly, enter a small buy order
            self.log.info(
                f"Detected significant ask absorption: {ask_absorption:.6f}",
                color=LogColor.BLUE,
            )
            
            # Calculate trade size as percentage of absorption with maximum limit
            trade_size = min(
                ask_absorption * self.config.trade_pct_of_absorption,
                float(self.config.max_trade_size)
            )
            
            # Get best ask price (we'll buy at this price)
            best_ask = book.best_ask_price()
            if best_ask:
                # Create and submit the buy order
                trade_qty = self.instrument.make_qty(Quantity(trade_size, self.instrument.size_precision))
                order = self.order_factory.limit(
                    instrument_id=self.instrument.id,
                    price=self.instrument.make_price(best_ask),
                    order_side=OrderSide.BUY,
                    quantity=trade_qty,
                    post_only=False,
                    time_in_force=TimeInForce.FOK,
                )
                
                self._last_trade_timestamp = self.clock.utc_now()
                self.log.info(f"Submitting buy order: {order}", color=LogColor.BLUE)
                self.submit_order(order)

    def on_stop(self) -> None:
        """
        Actions to be performed when the strategy is stopped.
        """
        if self.instrument is None:
            return

        self.log.info("Stopping Absorption Strategy")
        
        # Cancel any open orders and close positions
        self.cancel_all_orders(self.instrument.id)
        self.close_all_positions(self.instrument.id)
        
        # Unsubscribe from order book deltas
        self.unsubscribe_order_book_deltas(self.instrument.id, self.book_type)
