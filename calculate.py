#!/usr/bin/python

import pandas as pd
import os
from glob import glob

base_dir = "/var/www/html/toyota"
parquet_file_name = "dealer_markups.parquet"
parquet_file_path = os.path.join(base_dir, parquet_file_name)
excel_file_name = "dealer_markups.xlsx"
excel_file_path = os.path.join(base_dir, excel_file_name)

# Get a list of all the Parquet files, sorted by date
filepaths = sorted(glob(base_dir + "/*/*.parquet"),
                   key=os.path.getmtime, reverse=True)

# Empty DataFrame to store results
result_df = pd.DataFrame()

# Keep track of the number of cars per dealership
dealership_counts = {}

# Iterate over each file
for filepath in filepaths:
    # Read Parquet file into Pandas DataFrame
    df = pd.read_parquet(filepath, engine='pyarrow')

    # Filter rows where 'price.advertizedPrice' and 'price.totalMsrp' are not null
    df = df[df['price.advertizedPrice'].notnull() & df['price.totalMsrp'].notnull()]

    df['markup'] = df['price.advertizedPrice']-df['price.totalMsrp']

    # Group by dealership and take the top 10 cars based on 'markup'
    for dealership, group in df.groupby('dealerMarketingName'):
        if dealership not in dealership_counts:
            dealership_counts[dealership] = 0

        add_rows = min(10 - dealership_counts[dealership], len(group))
        if add_rows > 0:
            result_df = pd.concat(
                [result_df, group.nlargest(add_rows, 'markup')])  # type: ignore

            dealership_counts[dealership] += add_rows

    # If we've found 10 cars for each dealership, we can stop
    if all(count >= 10 for count in dealership_counts.values()):
        break

# Now 'result_df' should hold the 10 cars with highest 'markup' for each dealership

# Empty DataFrame to store final results
final_df = pd.DataFrame()

# Group 'result_df' by dealership and calculate required statistics
grouped = result_df.groupby('dealerMarketingName')
final_df['number_of_cars_with_markup'] = grouped.size()
final_df['minimum_markup'] = grouped['markup'].min()
final_df['average_markup'] = grouped['markup'].mean()
final_df['maximum_markup'] = grouped['markup'].max()

# Reset the index
final_df.reset_index(level=0, inplace=True)

final_df.to_parquet(parquet_file_path)
final_df.to_excel(excel_file_path)
