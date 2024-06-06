# Arrow Dataset Benchmarks

This repository contains code for benchmarking the use of Parquet `_metadata` files when reading from an Arrow Dataset.

## Local file system benchmarks

Generate data:
```
python write.py
```

Clear file system caches then run the read query:
```
sudo sh -c 'echo 3 >/proc/sys/vm/drop_caches'
python read_benchmark.py
```

Run using the `_metadata` file based Dataset:
```
sudo sh -c 'echo 3 >/proc/sys/vm/drop_caches'
python read_benchmark.py -m
```

## Minio (S3 API) benchmarks

Run a minio service:
```
podman run -p 9000:9000 -p 9001:9001 quay.io/minio/minio server /data --console-address ":9001"
```

Generate data:
```
python write.py --s3
```

Clear file system caches then run the read query:
```
sudo sh -c 'echo 3 >/proc/sys/vm/drop_caches'
python read_benchmark.py --s3
```

Run using the `_metadata` file based Dataset:
```
sudo sh -c 'echo 3 >/proc/sys/vm/drop_caches'
python read_benchmark.py --s3 -m
```

## Results

| File system | Baseline query time (ms) | Query time with `_metadata` (ms) | Speed up |
| --- | --- | --- | --- |
| Local (SSD) | 176 | 93 | 1.9x |
| Minio | 786 | 121 | 6.5x |
