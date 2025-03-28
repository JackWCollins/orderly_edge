from nautilus_trader.core.message import Event
from nautilus_trader.indicators.macd import MovingAverageConvergenceDivergence
from nautilus_trader.model import InstrumentId
from nautilus_trader.model import Position
from nautilus_trader.model.enums import OrderSide
from nautilus_trader.model.enums import PositionSide
from nautilus_trader.model.enums import PriceType
from nautilus_trader.model.events import PositionOpened
from nautilus_trader.trading.strategy import Strategy
from nautilus_trader.trading.strategy import StrategyConfig


class MACDConfig(StrategyConfig):
    instrument_id: InstrumentId
    fast_period: int = 12
    slow_period: int = 26
    trade_size: int = 1_000_000
    entry_threshold: float = 0.00010


class MACDStrategy(Strategy):
    def __init__(self, config: MACDConfig):
        super().__init__(config=config)
        # Our "trading signal"
        self.macd = MovingAverageConvergenceDivergence(
            fast_period=config.fast_period, slow_period=config.slow_period, price_type=PriceType.MID
        )

        self.trade_size = Quantity.from_int(config.trade_size)

        # Convenience
        self.position: Position | None = None

    def on_start(self):
        self.subscribe_quote_ticks(instrument_id=self.config.instrument_id)

    def on_stop(self):
        self.close_all_positions(self.config.instrument_id)
        self.unsubscribe_quote_ticks(instrument_id=self.config.instrument_id)

    def on_quote_tick(self, tick: QuoteTick):
        # You can register indicators to receive quote tick updates automatically,
        # here we manually update the indicator to demonstrate the flexibility available.
        self.macd.handle_quote_tick(tick)

        if not self.macd.initialized:
            return  # Wait for indicator to warm up

        # self._log.info(f"{self.macd.value=}:%5d")
        self.check_for_entry()
        self.check_for_exit()

    def on_event(self, event: Event):
        if isinstance(event, PositionOpened):
            self.position = self.cache.position(event.position_id)

    def check_for_entry(self):
        # If MACD line is above our entry threshold, we should be LONG
        if self.macd.value > self.config.entry_threshold:
            if self.position and self.position.side == PositionSide.LONG:
                return  # Already LONG

            order = self.order_factory.market(
                instrument_id=self.config.instrument_id,
                order_side=OrderSide.BUY,
                quantity=self.trade_size,
            )
            self.submit_order(order)
        # If MACD line is below our entry threshold, we should be SHORT
        elif self.macd.value < -self.config.entry_threshold:
            if self.position and self.position.side == PositionSide.SHORT:
                return  # Already SHORT

            order = self.order_factory.market(
                instrument_id=self.config.instrument_id,
                order_side=OrderSide.SELL,
                quantity=self.trade_size,
            )
            self.submit_order(order)

    def check_for_exit(self):
        # If MACD line is above zero then exit if we are SHORT
        if self.macd.value >= 0.0:
            if self.position and self.position.side == PositionSide.SHORT:
                self.close_position(self.position)
        # If MACD line is below zero then exit if we are LONG
        else:
            if self.position and self.position.side == PositionSide.LONG:
                self.close_position(self.position)

    def on_dispose(self):
        pass  # Do nothing else