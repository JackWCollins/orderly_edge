import datetime
from decimal import Decimal

from nautilus_trader.adapters.databento import DATABENTO
from nautilus_trader.adapters.databento import DATABENTO_CLIENT_ID
from nautilus_trader.adapters.databento import DatabentoDataClientConfig
from nautilus_trader.adapters.databento import DatabentoLiveDataClientFactory
from nautilus_trader.model.book import OrderBook
from nautilus_trader.trading.strategy import Strategy, StrategyConfig
from nautilus_trader.model.enums import OrderSide, TimeInForce, LogColor, BookType
from nautilus_trader.model.identifiers import InstrumentId

class LiquidityTakerConfig(StrategyConfig):
    """
    Configuration for the LiquidityTaker strategy.

    Parameters
    ----------
    instrument_id : InstrumentId
        The instrument to trade.
    cvd_threshold : float, default 5000.0
        The cumulative volume delta threshold.
    price_level_threshold : float, default 150.0
        Minimum ask price level at which to trigger a trade.
    wait_minutes_after_open : int, default 30
        The number of minutes to wait after market open before trading.
    trade_size : float, default 1.0
        The quantity to trade per order.
    session_start_time : str, default "09:30"
        The market open time represented as HH:MM (24-hour format).
    """
    instrument_id: InstrumentId
    cvd_threshold: float = 5000.0
    price_level_threshold: float = 150.0
    wait_minutes_after_open: int = 30
    trade_size: float = 1.0
    session_start_time: str = "09:30"


class LiquidityTaker(Strategy):
    def __init__(self, config: LiquidityTakerConfig) -> None:
        super().__init__(config)
        self.config = config
        self.cvd: float = 0.0  # cumulative volume delta
        self.session_date: datetime.date = self.clock.utc_now().date()
        self.market_open_time: datetime.datetime = self._get_market_open_time(self.session_date)

    def _get_market_open_time(self, session_date: datetime.date) -> datetime.datetime:
        # Parse the session start (market open) time from config.
        hour, minute = map(int, self.config.session_start_time.split(":"))
        return datetime.datetime.combine(session_date, datetime.time(hour=hour, minute=minute))

    def on_start(self) -> None:
        self.log.info("LiquidityTaker strategy starting.", LogColor.BLUE)
        # Reset cumulative volume and trading flag at the start.
        self.cvd = 0.0
        self.session_date = self.clock.utc_now().date()
        self.market_open_time = self._get_market_open_time(self.session_date)
        # Subscribe to trade ticks and order book updates.
        self.subscribe_trade_ticks(self.config.instrument_id, client_id=DATABENTO_CLIENT_ID)
        self.subscribe_order_book_at_interval(
            instrument_id=self.config.instrument_id,
            book_type=BookType.L2_MBP,
            depth=10,
            client_id=DATABENTO_CLIENT_ID,
            interval_ms=1000,
        )

    def on_trade_tick(self, tick) -> None:
        current_time = self.clock.utc_now()
        current_date = current_time.date()
        # Reset the session if a new day has begun.
        if current_date != self.session_date:
            self.session_date = current_date
            self.market_open_time = self._get_market_open_time(current_date)
            self.cvd = 0.0
            self.traded = False
            self.log.info("New trading session started. Resetting CVD.", LogColor.BLUE)

        # Update the cumulative volume delta.
        # (Assumes each trade tick has 'order_side' and 'size' attributes.)
        if hasattr(tick, "order_side") and hasattr(tick, "size"):
            if tick.order_side == OrderSide.BUY:
                self.cvd += float(tick.size)
            elif tick.order_side == OrderSide.SELL:
                self.cvd -= float(tick.size)
            self.log.debug(f"Updated CVD: {self.cvd}", LogColor.CYAN)

    def on_order_book(self, order_book: OrderBook) -> None:
        # Only trade after the wait period has elapsed.
        current_time = self.clock.utc_now()
        if current_time < self.market_open_time + datetime.timedelta(minutes=self.config.wait_minutes_after_open):
            return

        target_price = None
        # Look for an ask level whose price is greater than the configured threshold.
        for level in order_book.asks():
            # Assumes each level has a 'price' attribute.
            if level.price > Decimal(self.config.price_level_threshold):
                target_price = level.price
                break

        if target_price is None:
            return

        # Decide on the order side based on the CVD.
        if self.cvd > self.config.cvd_threshold:
            # Bullish condition: Place a market BUY.
            self.log.info(
                f"CVD ({self.cvd}) > {self.config.cvd_threshold}. "
                f"Found ask level at {target_price}. Placing market BUY order.",
                LogColor.GREEN
            )
            order = self.order_factory.market(
                instrument_id=self.config.instrument_id,
                order_side=OrderSide.BUY,
                quantity=self.config.trade_size,
                time_in_force=TimeInForce.FOK,
            )
        elif self.cvd < self.config.cvd_threshold:
            # Bearish condition: Place a market SELL.
            self.log.info(
                f"CVD ({self.cvd}) < {self.config.cvd_threshold}. "
                f"Found ask level at {target_price}. Placing market SELL order.",
                LogColor.RED
            )
            order = self.order_factory.market(
                instrument_id=self.config.instrument_id,
                order_side=OrderSide.SELL,
                quantity=self.config.trade_size,
                time_in_force=TimeInForce.FOK,
            )
        else:
            return

        self.submit_order(order)
        self.log.info(f"Order submitted: {order}. Target price set at {target_price}.", LogColor.BLUE)
        self.traded = True

    def on_stop(self) -> None:
        self.log.info("LiquidityTaker strategy stopped.", LogColor.BLUE)