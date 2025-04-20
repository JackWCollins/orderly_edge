import os
from dotenv import load_dotenv

from nautilus_trader.adapters.databento.data_utils import init_databento_client, databento_data
import nautilus_trader.adapters.databento.data_utils as db_data_utils

load_dotenv()

db_data_utils.DATABENTO_API_KEY = os.getenv("DATABENTO_API_KEY")
init_databento_client()

databento_data(
    symbols=["ESM5"],
    start_time="2025-03-27T00:00:00",
    end_time="2025-03-27T01:00:00",
    schema="mbo",
    file_prefix="esm5-mbo",
    use_exchange_as_venue=True,
)

# path = "es-front-glbx-mbo.dbn.zst"

# print(f"DBN file exists: {os.path.exists(path)}")
# print(f"Path: {path}")

# if not path.exists():
#     # Request data
#     databento_data(
#         symbols=["ESU4"],
#         start_time="2024-07-08T00:00:00",
#         end_time="2024-07-08T01:00:00",
#         schema="mbo",
#         file_prefix="esu4-mbo",
#     )
#     # print(result)

# data = db.DBNStore.from_file("tests/test_data/databento/databento/es-front-glbx-mbo_mbo_2024-07-08T00h00h00_2024-07-08T01h00h00.dbn.zst")
# df = data.to_df()
# print(df)

# client = db.Historical()

# cost = client.metadata.get_cost(
#     dataset="GLBX.MDP3",
#     symbols=["ES.n.0"],
#     stype_in="continuous",
#     schema="mbp-10",
#     start="2024-12-06T14:30:00",
#     end="2024-12-06T20:30:00",
# )

# print(cost)

# path = DATABENTO_DATA_DIR / "es-front-glbx-mbp10.dbn.zst"


# instrument_id = InstrumentId.from_str("ESU4.GLBX")  # ES September 2024 contract

# catalog = ParquetDataCatalog(path=catalog_folder)

# dbn_file_path = "data/ES/202407/glbx-mdp3-20240701.mbo.dbn.zst"
# print(f"DBN file exists: {os.path.exists(dbn_file_path)}")

# loader = DatabentoDataLoader()

# try:
#     print(f"Loading data from {dbn_file_path}...")
#     data = loader.from_dbn_file(
#         path=dbn_file_path,
#         as_legacy_cython=False,  # Use pyo3 objects for optimization when writing to catalog
#     )
#     print(f"Loaded {len(data)} records from DBN file")
    
#     if data:
#         print(f"First record type: {type(data[0])}")
        
#         print(f"Writing data to catalog at {catalog_folder}...")
#         catalog.write_data(
#             data=data,
#             mode=CatalogWriteMode.OVERWRITE,
#         )
#         print(f"Wrote {len(data)} records to catalog at {catalog_folder}")
        
#         registered_instruments = catalog.instruments()
#         print("Catalog instruments:", registered_instruments)
#         if registered_instruments:
#             ins = registered_instruments[0]
#             print("Instrument:", ins)
#             data_types = catalog.data_types(ins.id)
#             print("Available data types:", data_types)
# except Exception as e:
#     print(f"Error processing DBN file: {e}")