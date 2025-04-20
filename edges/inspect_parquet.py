import pyarrow.parquet as pq

# Replace with the path to your catalog file or directory
# table = pq.read_table("tests/test_data/databento/data/order_book_delta/ESM5.GLBX/part-0.parquet")
table = pq.read_table("tests/test_data/databento/data/order_book_delta/ESM5.XCME/part-0.parquet")
# table = pq.read_table("tests/test_data/databento/data/futures_contract/ESM5.GLBX/part-0.parquet")

# View schema
# print(table.schema)
print(table.schema)

# View a sample
df = table.to_pandas()
print(df.head())
