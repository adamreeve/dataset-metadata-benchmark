import argparse
from pathlib import Path
import time

import pyarrow.dataset as ds
import pyarrow as pa
import pyarrow.compute as pc
from pyarrow import fs


parser = argparse.ArgumentParser()
parser.add_argument('-m', '--use-metadata', action='store_true')
parser.add_argument('--s3', action='store_true')
args = parser.parse_args()

if args.s3:
    file_system = fs.S3FileSystem(
            endpoint_override="localhost:9000",
            scheme="http",
            access_key="minioadmin",
            secret_key="minioadmin")
    dataset_path = Path("dataset")
else:
    file_system = None
    dataset_path = Path("./dataset")

dataset_path = Path("./dataset")
pformat = ds.ParquetFileFormat()
partitioning = ds.partitioning(
        schema=pa.schema([
            pa.field("year", pa.int16())
        ]),
        flavor="hive")
schema = pa.schema([
    pa.field("year", pa.int16()),
    pa.field("x", pa.int32()),
    pa.field("y", pa.int32()),
])

filter_expr = (pc.field('x') == 3)

t0 = time.perf_counter()

if args.use_metadata:
    dataset = ds.parquet_dataset(
        (dataset_path / "_metadata").as_posix(),
        format=pformat,
        schema=schema,
        partitioning=partitioning,
        filesystem=file_system)
else:
    dataset = ds.dataset(
        dataset_path.as_posix(),
        format=pformat,
        schema=schema,
        partitioning=partitioning,
        filesystem=file_system)

t1 = time.perf_counter()

result = dataset.filter(filter_expr).to_table().to_pandas().sort_values(['year', 'y'])

t2 = time.perf_counter()

print(f"Time to open dataset = {1000 * (t1 - t0):.3f} ms")
print(f"Time to read dataset = {1000 * (t2 - t1):.3f} ms")
