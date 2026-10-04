# Performance

Time to first rows and peak memory, measured with `scripts/bench/startup.py` on a
release build.

| Measure | Meaning |
|---|---|
| First rows | From launch to the first drawn frame that shows the file's rows |
| Peak RSS | The process's resident high-water mark (`VmHWM`), from launch until 2 s after the first rows |
| Warm | The file is already in the page cache |
| Cold | The file is dropped from the page cache first (`posix_fadvise` `DONTNEED`) |

## Results

datui 0.4.0-dev, median of 5 runs (remote: 3). Terminal 120×30.

| File | Cache | First rows | Peak RSS |
|---|---|---:|---:|
| Parquet, 1M rows (13 MB) | warm | 8 ms | 55 MiB |
| Parquet, 1M rows (13 MB) | cold | 11 ms | 55 MiB |
| CSV, 1M rows (58 MB) | warm | 12 ms | 173 MiB |
| CSV, 1M rows (58 MB) | cold | 14 ms | 172 MiB |
| Parquet, 10M rows (132 MB) | warm | 8 ms | 56 MiB |
| Parquet, 10M rows (132 MB) | cold | 11 ms | 55 MiB |
| CSV, 10M rows (586 MB) | warm | 12 ms | 688 MiB |
| CSV, 10M rows (586 MB) | cold | 14 ms | 688 MiB |
| Parquet, 30M rows (397 MB) | warm | 8 ms | 55 MiB |
| Parquet, 30M rows (397 MB) | cold | 10 ms | 55 MiB |
| CSV, 30M rows (1,779 MB) | warm | 10 ms | 1.43 GiB |
| CSV, 30M rows (1,779 MB) | cold | 11 ms | 1.38 GiB |
| [NYC yellow taxis, Jan 2025](https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-01.parquet) (HTTPS, 59 MB Parquet) | remote | 667 ms | 71 MiB |
| [NOAA GHCN-D 2023 TMAX](https://registry.opendata.aws/noaa-ghcn/) (`s3://noaa-ghcn-pds/parquet/by_year/YEAR=2023/ELEMENT=TMAX/`, 9 files) | remote | 540 ms | 72 MiB |

- Only the rows on screen are read before the first frame, so first rows does
  not grow with the file.
- CSV peak RSS grows with the file: the row count in the status bar reads the
  whole CSV in the 2 s after the first rows. A Parquet footer holds the count.
- The HTTPS row includes downloading the file and answering the download
  question. The S3 row reads the footers and the first row group in place.
- Remote rows depend on the network and the server.

## Machine

| | |
|---|---|
| CPU | AMD Ryzen 7 9800X3D, 8 cores, 16 threads |
| Memory | 62 GiB |
| Disk | Samsung 990 PRO NVMe, btrfs with zstd compression |
| OS | Arch Linux, kernel 7.2.7 |
| Date | October 2, 2026 |

[Run benchmarks](../for-developers/benchmarks.md) says how these are measured.
