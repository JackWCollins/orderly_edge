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

from nautilus_trader.adapters.databento.data_utils import init_databento_client
import nautilus_trader.adapters.databento.data_utils as db_data_utils

from nautilus_trader.backtest.node import BacktestEngineConfig
from nautilus_trader.backtest.node import BacktestEngine


from nautilus_trader.config import LoggingConfig
from nautilus_trader.config import ImportableStrategyConfig
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.data import OrderBookDeltas
from nautilus_trader.persistence.catalog import ParquetDataCatalog

from pathlib import Path

# Import the AbsorptionStrategy and its config
from edges.absorption import EnhancedAbsorptionStrategy
from edges.absorption import EnhancedAbsorptionConfig


from nautilus_trader.adapters.databento.loaders import DatabentoDataLoader
from nautilus_trader.config import LoggingConfig
from nautilus_trader.config import RiskEngineConfig
from nautilus_trader.model.currencies import USD
from nautilus_trader.model.data import BarType
from nautilus_trader.model.enums import AccountType
from nautilus_trader.model.enums import OmsType
from nautilus_trader.model.identifiers import TraderId
from nautilus_trader.model.identifiers import Venue
from nautilus_trader.model.objects import Money
from nautilus_trader.test_kit.providers import TestInstrumentProvider

import time

# %% [markdown]
# ## Parameters

# %%
# Load environment variables (for API keys)
load_dotenv()

# TEST_DATA_DIR = Path("tests/test_data/large/ES/2024_07/")
TEST_DATA_DIR = Path("tests/test_data/databento/databento/")

# Databento API configuration
db_data_utils.DATABENTO_API_KEY = os.getenv("DATABENTO_API_KEY")
init_databento_client()

# Instrument configuration
instrument_id = InstrumentId.from_str("ESM5.GLBX")  
# print(f"Instrument ID: {instrument_id}, venue: {instrument_id.venue}")
# %%


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

if __name__ == "__main__":
    # Configure backtest engine
    config = BacktestEngineConfig(
        trader_id=TraderId("BACKTESTER-001"),
        logging=LoggingConfig(log_level="INFO"),
        risk_engine=RiskEngineConfig(bypass=True),
    )

    # Build the backtest engine
    engine = BacktestEngine(config=config)

    # Add a trading venue (multiple venues possible)
    GLBX = Venue("GLBX")  # <-- ISO 10383 MIC
    engine.add_venue(
        venue=GLBX,
        oms_type=OmsType.NETTING,
        account_type=AccountType.CASH,
        base_currency=USD,
        starting_balances=[Money(1_000_000.0, USD)],
    )

    # Add instruments
    ESM5_GLBX = TestInstrumentProvider.equity(symbol="ESM5", venue="GLBX")
    engine.add_instrument(ESM5_GLBX)

    # Add data
    loader = DatabentoDataLoader()

    filenames = [
        # "glbx-mdp3-20240702.mbo.dbn.zst",
        "esm5-mbo_mbo_2025-03-27T00h00h00_2025-03-27T01h00h00.dbn.zst"
    ]

    for filename in filenames:
        data = loader.from_dbn_file(
            path=TEST_DATA_DIR / filename,
            instrument_id=ESM5_GLBX.id,
            # include_trades=True,
        )
        engine.add_data(data)

    # Configure your strategy
    config = EnhancedAbsorptionConfig(
        instrument_id=instrument_id,
        min_absorption_volume=100.0,
        liquidity_threshold=100.0,
        monitor_levels=40,
        cooldown_period_seconds=60.0,
        trade_size=Decimal(1.0),
    )

    # Instantiate and add your strategy
    strategy = EnhancedAbsorptionStrategy(config=config)
    engine.add_strategy(strategy=strategy)

    time.sleep(0.1)
    input("Press Enter to continue...")

    # Run the engine (from start to end of data)
    engine.run()

    # Optionally view reports
    with pd.option_context(
        "display.max_rows",
        100,
        "display.max_columns",
        None,
        "display.width",
        300,
    ):
        print(engine.trader.generate_account_report())
        print(engine.trader.generate_order_fills_report())
        print(engine.trader.generate_positions_report())

    # For repeated backtest runs make sure to reset the engine
    engine.reset()

    # Good practice to dispose of the object
    engine.dispose()
