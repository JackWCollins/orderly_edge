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

# %% [markdown]
# ## Step 1: Load DBN data and write to catalog

# %%
# Check if the DBN file exists
dbn_file_path = "data/ES/202407/glbx-mdp3-20240701.mbo.dbn.zst"
print(f"DBN file exists: {os.path.exists(dbn_file_path)}")

# Create the loader and load the data
loader = DatabentoDataLoader()

# Load the DBN file
try:
    data = loader.from_dbn_file(
        path=dbn_file_path,
        instrument_id=instrument_id,
        as_legacy_cython=False,  # Use pyo3 objects for optimization when writing to catalog
    )
    print(f"Loaded {len(data)} records from DBN file")
    
    if data and len(data) > 0:
        print(f"First record type: {type(data[0])}")
except Exception as e:
    print(f"Error loading DBN file: {e}")
    # Continue with the rest of the code even if loading fails
    data = []

# Create or load the catalog
catalog = ParquetDataCatalog(path=catalog_folder)

# Write the data to the catalog
if data:
    catalog.write_data(
        data=data,
        mode=CatalogWriteMode.OVERWRITE,
    )
    print(f"Wrote {len(data)} records to catalog at {catalog_folder}")

# %%
# Configure logging
logging = LoggingConfig(
    log_level="INFO",
    use_pyo3=True,
)