import os
import pandas as pd
from dotenv import load_dotenv
from decimal import Decimal

from nautilus_trader.adapters.databento.loaders import DatabentoDataLoader
from nautilus_trader.adapters.databento.data_utils import init_databento_client
import nautilus_trader.adapters.databento.data_utils as db_data_utils
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.persistence.catalog import ParquetDataCatalog
from nautilus_trader.persistence.catalog.types import CatalogWriteMode

# Import instrument creation tools
from nautilus_trader.model.objects import Price, Quantity
from nautilus_trader.model.currencies import USD

load_dotenv()

catalog_folder = "es_mbo_catalog"

db_data_utils.DATABENTO_API_KEY = os.getenv("DATABENTO_API_KEY")
init_databento_client()

instrument_id = InstrumentId.from_str("ESU4.GLBX")  # ES September 2024 contract

catalog = ParquetDataCatalog(path=catalog_folder)

dbn_file_path = "data/ES/202407/glbx-mdp3-20240701.mbo.dbn.zst"
print(f"DBN file exists: {os.path.exists(dbn_file_path)}")

loader = DatabentoDataLoader()

try:
    print(f"Loading data from {dbn_file_path}...")
    data = loader.from_dbn_file(
        path=dbn_file_path,
        as_legacy_cython=False,  # Use pyo3 objects for optimization when writing to catalog
    )
    print(f"Loaded {len(data)} records from DBN file")
    
    if data:
        print(f"First record type: {type(data[0])}")
        
        print(f"Writing data to catalog at {catalog_folder}...")
        catalog.write_data(
            data=data,
            mode=CatalogWriteMode.OVERWRITE,
        )
        print(f"Wrote {len(data)} records to catalog at {catalog_folder}")
        
        registered_instruments = catalog.instruments()
        print("Catalog instruments:", registered_instruments)
        if registered_instruments:
            ins = registered_instruments[0]
            print("Instrument:", ins)
            data_types = catalog.data_types(ins.id)
            print("Available data types:", data_types)
except Exception as e:
    print(f"Error processing DBN file: {e}")