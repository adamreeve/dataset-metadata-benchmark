from pathlib import Path
import time

import pyarrow as pa
from pyiceberg.catalog.sql import SqlCatalog


schema = pa.schema([
    pa.field("year", pa.int16()),
    pa.field("x", pa.int32()),
    pa.field("y", pa.int32()),
])

t0 = time.perf_counter()

warehouse_path = Path("./iceberg_warehouse").absolute().as_posix()
catalog = SqlCatalog(
    "default",
    uri=f"sqlite:///{warehouse_path}/pyiceberg_catalog.db",
    warehouse=f"file://{warehouse_path}",
)

table = catalog.load_table(
    "default.benchmark_dataset",
)

t1 = time.perf_counter()

df = table.scan(row_filter="x == 3").to_arrow()

t2 = time.perf_counter()

print(f"Time to open dataset = {1000 * (t1 - t0):.3f} ms")
print(f"Time to read dataset = {1000 * (t2 - t1):.3f} ms")

print(f"Query returned {len(df)} rows")
