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

# %% [markdown]
# ## Databento Data Catalog Setup and Backtest with Absorption Strategy
# 
# This file demonstrates:
# 1. Loading Databento DBN format data
# 2. Writing it to a Parquet catalog for efficient reuse
# 3. Running a backtest using the catalog data with AbsorptionStrategy

# %% [markdown]
# ## Imports

# %%
# Note: Use the python extension jupytext to be able to open this python file in jupyter as a notebook
import os
import pandas as pd
from decimal import Decimal
from dotenv import load_dotenv

from nautilus_trader.adapters.databento import DATABENTO
from nautilus_trader.adapters.databento.loaders import DatabentoDataLoader
from nautilus_trader.adapters.databento.data_utils import init_databento_client
from nautilus_trader.adapters.databento.data_utils import load_catalog
import nautilus_trader.adapters.databento.data_utils as db_data_utils

from nautilus_trader.common.enums import LogColor
from nautilus_trader.backtest.node import BacktestDataConfig
from nautilus_trader.backtest.node import BacktestEngineConfig
from nautilus_trader.backtest.node import BacktestNode
from nautilus_trader.backtest.node import BacktestRunConfig
from nautilus_trader.backtest.node import BacktestVenueConfig
from nautilus_trader.config import LoggingConfig
from nautilus_trader.config import ImportableStrategyConfig
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.identifiers import TraderId
from nautilus_trader.model.identifiers import Venue
from nautilus_trader.persistence.catalog import ParquetDataCatalog
from nautilus_trader.persistence.catalog.types import CatalogWriteMode
from nautilus_trader.trading.strategy import Strategy

# Import the AbsorptionStrategy and its config
from edges.absorption import EnhancedAbsorptionStrategy
from edges.absorption import EnhancedAbsorptionConfig

# %% [markdown]
# ## Parameters

# %%
# Load environment variables (for API keys)
load_dotenv()

# Catalog configuration
catalog_folder = "es_mbo_catalog"

# Databento API configuration
db_data_utils.DATABENTO_API_KEY = os.getenv("DATABENTO_API_KEY")
init_databento_client()

# Instrument configuration
instrument_id = InstrumentId.from_str("ESU4.GLBX")  # ES September 2024 contract

# Create or load the catalog
# catalog = ParquetDataCatalog(path=catalog_folder)
# print(f"Catalog instruments: {catalog.instruments()}")
# instrument = catalog.instruments()[0]
# print(f"Instrument: {instrument}")

# %%
# Configure logging
logging = LoggingConfig(
    log_level="INFO",
    use_pyo3=True,
)

venue_configs = [
    BacktestVenueConfig(
        name="GLBX",
        oms_type="HEDGING",
        account_type="MARGIN",
        base_currency="USD",
        starting_balances=["100_000 USD"],
        book_type="L3_MBO",
    ),
]
# Configure the data client to use the catalog
data_configs = [
  BacktestDataConfig(
      catalog_path=catalog_folder,
      instrument_id=instrument_id,
      data_cls="nautilus_trader.core.nautilus_pyo3.model:OrderBookDelta",
      start_time=pd.Timestamp("2024-07-01 00:00:00", tz="UTC"),
      end_time=pd.Timestamp("2024-07-01 23:59:59", tz="UTC"),
  )
]

# Add the strategy
strategies = [
    ImportableStrategyConfig(
        strategy_path="edges.absorption:EnhancedAbsorptionStrategy",
        config_path="edges.absorption:EnhancedAbsorptionConfig",
        config={
            "instrument_id": instrument_id,
            "min_absorption_volume": 10.0,
            "liquidity_threshold": 20.0,
            "monitor_levels": 5,
            "cooldown_period_seconds": 10.0,
            "trade_size": Decimal(1.0),
        },
    ),
]

config = BacktestRunConfig(
    engine=BacktestEngineConfig(strategies=strategies),
    data=data_configs,
    venues=venue_configs,
)

# Create the backtest node
backtest_node = BacktestNode(configs=[config])

# %% [markdown]
# ## Run the backtest
results = backtest_node.run()
results


# %%
# Stop the node when done
backtest_node.dispose()