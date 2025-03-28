from typing import Any

from nautilus_trader.adapters.databento import DATABENTO
from nautilus_trader.adapters.databento import DATABENTO_CLIENT_ID
from nautilus_trader.adapters.databento import DatabentoDataClientConfig
from nautilus_trader.adapters.databento import DatabentoLiveDataClientFactory
from nautilus_trader.cache.config import CacheConfig
from nautilus_trader.common.config import DatabaseConfig
from nautilus_trader.common.enums import LogColor
from nautilus_trader.config import InstrumentProviderConfig
from nautilus_trader.config import LiveExecEngineConfig
from nautilus_trader.config import LoggingConfig
from nautilus_trader.config import StrategyConfig
from nautilus_trader.config import TradingNodeConfig
from nautilus_trader.live.node import TradingNode
from nautilus_trader.model.enums import BookType
from nautilus_trader.model.book import OrderBook
from nautilus_trader.model.data import Bar
from nautilus_trader.model.data import OrderBookDeltas
from nautilus_trader.model.data import QuoteTick
from nautilus_trader.model.data import TradeTick
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.identifiers import TraderId
from nautilus_trader.trading.strategy import Strategy
from dotenv import load_dotenv

import os

load_dotenv()

instrument_ids = [
    InstrumentId.from_str("ESM5.GLBX"),
]

print(f"instrument_ids: {instrument_ids}")

# Configure the trading node
config_node = TradingNodeConfig(
    trader_id=TraderId("TRADER-001"),
    data_clients={
        DATABENTO: DatabentoDataClientConfig(
          api_key=os.getenv("DATABENTO_API_KEY"),
          http_gateway=None,
          mbo_subscriptions_delay=10.0,
          instrument_ids=instrument_ids,
          parent_symbols={"GLBX.MDP3": {"ES.FUT"}},
        ),
    },
)

# Instantiate the node with a configuration
node = TradingNode(config=config_node)


class TestStrategyConfig(StrategyConfig, frozen=True):
    """
    Configuration for ``TestStrategy`` instances.

    Parameters
    ----------
    instrument_ids : list[InstrumentId]
        The instrument IDs to subscribe to.

    """

    instrument_ids: list[InstrumentId]


class TestStrategy(Strategy):
    """
    An example strategy which subscribes to live data.

    Parameters
    ----------
    config : TestStrategyConfig
        The configuration for the instance.

    """

    def __init__(self, config: TestStrategyConfig) -> None:
        super().__init__(config)

    def on_start(self) -> None:
        """
        Actions to be performed when the strategy is started.

        Here we specify the 'DATABENTO' client_id for subscriptions.

        """
        for instrument_id in self.config.instrument_ids:
            # This does not work 
            self.subscribe_order_book_deltas(
                instrument_id=instrument_id,
                book_type=BookType.L3_MBO,
                client_id=DATABENTO_CLIENT_ID,
                managed=False
            )
            # Both of these work
            # self.subscribe_order_book_at_interval(
            #     instrument_id=instrument_id,
            #     book_type=BookType.L2_MBP,
            #     depth=10,
            #     client_id=DATABENTO_CLIENT_ID,
            #     interval_ms=1000,
            # )

            # self.subscribe_trade_ticks(instrument_id, client_id=DATABENTO_CLIENT_ID)

    def on_stop(self) -> None:
        """
        Actions to be performed when the strategy is stopped.
        """
        # Databento does not support live data unsubscribing

    def on_historical_data(self, data: Any) -> None:
        self.log.info(repr(data), LogColor.CYAN)

    def on_order_book_deltas(self, deltas: OrderBookDeltas) -> None:
        """
        Actions to be performed when the strategy is running and receives order book
        deltas.

        Parameters
        ----------
        deltas : OrderBookDeltas
            The order book deltas received.

        """
        self.log.info(repr(deltas), LogColor.CYAN)

    def on_order_book(self, order_book: OrderBook) -> None:
        """
        Actions to be performed when an order book update is received.
        """
        self.log.info(f"\n{order_book.instrument_id}\n{order_book.pprint(10)}", LogColor.CYAN)

    def on_quote_tick(self, tick: QuoteTick) -> None:
        """
        Actions to be performed when the strategy is running and receives a quote tick.

        Parameters
        ----------
        tick : QuoteTick
            The tick received.

        """
        self.log.info(repr(tick), LogColor.CYAN)

    def on_trade_tick(self, tick: TradeTick) -> None:
        """
        Actions to be performed when the strategy is running and receives a trade tick.

        Parameters
        ----------
        tick : TradeTick
            The tick received.

        """
        self.log.info(repr(tick), LogColor.CYAN)

    def on_bar(self, bar: Bar) -> None:
        """
        Actions to be performed when the strategy is running and receives a bar.

        Parameters
        ----------
        bar : Bar
            The bar received.

        """
        self.log.info(repr(bar), LogColor.CYAN)


# Configure and initialize your strategy
strat_config = TestStrategyConfig(instrument_ids=instrument_ids)
strategy = TestStrategy(config=strat_config)

# Add your strategies and modules
node.trader.add_strategy(strategy)

# Register your client factories with the node (can take user-defined factories)
node.add_data_client_factory(DATABENTO, DatabentoLiveDataClientFactory)
print("Building node")
node.build()

# Stop and dispose of the node with SIGINT/CTRL+C
if __name__ == "__main__":
    try:
        print("Running node")
        node.run()
    finally:
        node.dispose()
