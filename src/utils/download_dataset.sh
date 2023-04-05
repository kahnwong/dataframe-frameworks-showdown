#!/bin/bash

## source: https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
## download URL for 2023-01
# https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-01.parquet

for year in $(seq -w 2009 2022); do
	for month in $(seq -w 1 12); do
		download_dir="data/nyc-trip-data/year=$year/month=$month"
		mkdir -p "$download_dir"

		download_url="https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_$year-$month.parquet"
		wget --directory-prefix "$download_dir" "$download_url"
	done
done
