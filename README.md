# Dataframe Frameworks Showdown

## Frameworks Used

- ~~pandas~~ (not present in the experiment, because it's very unrealistic to expect pandas to be able to open 15GB data on a 16GB RAM machine)
- polars
- duckdb
- spark (single-node)

## Experiments

query: filter by percentiles + groupby

| framework | mode      |
| --------- | --------- |
| polars    | default   |
| duckdb    | default   |
| spark     | default   |
| spark     | optimized |

## Compute specs

- CPU: M1 `MacBook Air (M1, 2020)`
- RAM: 16GB

## Data source

See [here](src/utils/download_dataset.sh).

- total size: `15 GB`,
- total records: `1,195,313,202 - around 1200 million rows`
- partitions: `year 2012` to `year 2022` (older partitions have different schema)
- **dirty data**: some columns have mismatched data types across partitions

## Usage

```bash
# download data
make download-data

# run experiments
make run
```
