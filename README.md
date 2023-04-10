# Dataframe Frameworks Showdown

Blog post at <https://www.karnwong.me/posts/2023/04/duckdb-vs-polars-vs-spark/>

## Frameworks Used

- ~~pandas~~ (not present in the experiment, because it's very unrealistic to expect pandas to be able to open 15GB data on a 16GB RAM machine)
- polars
- duckdb
- spark (single node)

## Experiments

Input data (of varying row count, to measure performance against same partition size but different length) is equally partitioned into 8 chunks.

1. Create a timestamp diff column `trip_length_minute`, converted to minute
2. Create percentile on `trip_length_minute` as `trip_length_minute_percentile`
3. Filter only `trip_length_minute_percentile` between (0.2, 0.8)
4. Group by on `VendorID`, `payment_type`
5. Aggregate min, max, avg on `passenger_count`, `trip_distance`, `total_amount`

| framework | mode    | remarks                                          |
| --------- | ------- | ------------------------------------------------ |
| polars    | lazy    | by default, polars does not operate in lazy mode |
| duckdb    | default | -                                                |
| spark     | default | -                                                |

## Compute specs

- CPU: M1 `MacBook Air (M1, 2020)`
- RAM: 16GB

## Data source

See [here](src/utils/download_dataset.sh).

- total size: `15 GB`,
- total records: `1,195,313,202 - around 1200 million rows`
- partitions: `year 2012` to `year 2022` (older partitions have different schema)
- **dirty data**: some columns have mismatched data types across partitions

These will be partitioned and used for experiments. See `prep` stage in `Makefile`.

## Usage

```bash
# download data
make download-data

# sanitize data
make prep-base

# run experiment 1: window on single partition
# contains prep, run and visualize
make run-expt1-window-on-single-partition

# run experiment 2: window on multiple partitions
make run-expt2-window-on-multiple-partition
```

## Results

- `duckdb` crash/unrespond at 30M rows (task terminated by user at 14 minute).

![result expt1](images/result_expt1.png)

![result expt2](images/result_expt2.png)
