import argparse
from pathlib import Path

import pyarrow.parquet as pq
import pyarrow.dataset as ds
import pyarrow as pa
from pyarrow import fs
import numpy as np


parser = argparse.ArgumentParser()
parser.add_argument('--s3', action="store_true")
args = parser.parse_args()

if args.s3:
    file_system = fs.S3FileSystem(
            endpoint_override="localhost:9000",
            scheme="http",
            allow_bucket_creation=True,
            access_key="minioadmin",
            secret_key="minioadmin")
    dataset_path = Path("dataset")
else:
    file_system = None
    dataset_path = Path("./dataset")
    dataset_path.mkdir(exist_ok=False)

num_years = 10
rows_per_batch = 10_000
num_leaf_directory_files = 100
files_per_x = 2  # Number of leaf files per directory that contain data for each unique x value

schema = pa.schema([
    pa.field("year", pa.int16()),
    pa.field("x", pa.int32()),
    pa.field("y", pa.int32()),
])

rng = np.random.default_rng()
metadata_collector = []

for write_num in range(num_leaf_directory_files):
    batches = []
    for year_num in range(num_years):
        batches.append(pa.RecordBatch.from_arrays(
                [
                    np.repeat(2000 + year_num, rows_per_batch),
                    rng.integers(write_num, write_num + files_per_x, rows_per_batch),
                    rng.integers(0, 1_000, rows_per_batch),
                ],
                schema=schema))
    table = pa.Table.from_batches(batches)

    # write_to_dataset generates a new uuid on each call to use in the file name
    # format, so appends new data to the dataset.
    pq.write_to_dataset(
        table,
        dataset_path.as_posix(),
        filesystem=file_system,
        partitioning=ds.partitioning(
            schema=pa.schema([
                pa.field("year", pa.int16())
            ]),
            flavor="hive"
        ),
        metadata_collector=metadata_collector
    )

assert schema is not None

pq.write_metadata(
    pa.schema(
        field
        for field in schema
        if field.name != "year"
    ),
    (dataset_path / "_metadata").as_posix(),
    metadata_collector,
    filesystem=file_system)
