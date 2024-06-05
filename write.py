from pathlib import Path

import pyarrow.parquet as pq
import pyarrow.dataset as ds
import pyarrow as pa
import numpy as np


dataset_path = Path("./dataset")
dataset_path.mkdir(exist_ok=False)

num_years = 5
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
        dataset_path,
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
    dataset_path / "_metadata",
    metadata_collector
)
