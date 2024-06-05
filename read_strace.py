import argparse
import os
from pathlib import Path
import subprocess
import time

import pyarrow.dataset as ds
import pyarrow as pa
import pyarrow.compute as pc


parser = argparse.ArgumentParser()
parser.add_argument('-m', '--use-metadata', action='store_true')
args = parser.parse_args()


dataset_path = Path("./dataset")

pformat = ds.ParquetFileFormat()

partitioning = ds.partitioning(
        schema=pa.schema([
            pa.field("year", pa.int16())
        ]),
        flavor="hive"
    )
schema = pa.schema([
    pa.field("year", pa.int16()),
    pa.field("x", pa.int32()),
    pa.field("y", pa.int32()),
])

filter_expr = (pc.field('x') == 3) & (pc.field('year') == 2002)

print("Opening dataset", flush=True)

process_id = os.getpid()
strace = subprocess.Popen(['strace', '-f', '-t', '-e', 'trace=file', '-p', str(process_id)])
time.sleep(2)

if args.use_metadata:
    dataset = ds.parquet_dataset(
        dataset_path / "_metadata",
        format=pformat,
        schema=schema,
        partitioning=partitioning
    )
else:
    dataset = ds.dataset(
        dataset_path,
        format=pformat,
        schema=schema,
        partitioning=partitioning
    )

print("Scanning dataset with filter", flush=True)

result = dataset.filter(filter_expr).to_table().to_pandas().sort_values(['year', 'y'])

strace.kill()
strace.wait(timeout=60)
