#!/usr/bin/python

import pandas as pd
import os
from glob import glob
import re

base_dir = "/var/www/html/toyota"
parquet_file_name = "dealer_markups.parquet"
excel_file_name = "dealer_markups.xlsx"

# Change 1: Add a regular expression to match directories and files of format 'YYYY-MM-DD'
regex = re.compile(r'\d{4}-\d{2}-\d{2}')

# Get a list of all the Parquet files in YYYY-MM-DD directories, sorted by date
filepaths = sorted([fp for fp in glob(base_dir + "/*/*.parquet")
                    if regex.fullmatch(fp.split('/')[-2]) and regex.fullmatch(fp.split('/')[-1].split('.')[0])],
                   key=os.path.getmtime, reverse=True)

# Get the directory path of the latest file for saving output files
latest_dir = os.path.dirname(filepaths[0])
parquet_file_path = os.path.join(latest_dir, parquet_file_name)
excel_file_path = os.path.join(latest_dir, excel_file_name)

# Empty DataFrame to store results
result_df = pd.DataFrame()

# Keep track of the number of cars per dealership
dealership_counts = {}

# Iterate over each file
for filepath in filepaths:
    # Read Parquet file into Pandas DataFrame
    df = pd.read_parquet(filepath, engine='pyarrow')

    # Filter rows where 'markup' is not null
    df = df[df['markup'].notnull()]

    # Group by dealership and take the top 10 cars based on 'markup'
    for dealership, group in df.groupby('dealerMarketingName'):
        if dealership not in dealership_counts:
            dealership_counts[dealership] = 0

        add_rows = min(10 - dealership_counts[dealership], len(group))
        if add_rows > 0:
            result_df = pd.concat(
                [result_df, group.nlargest(add_rows, 'markup')])

            dealership_counts[dealership] += add_rows

    # If we've found 10 cars for each dealership, we can stop
    if all(count >= 10 for count in dealership_counts.values()):
        break

# Now 'result_df' should hold the 10 cars with highest 'markup' for each dealership

# Empty DataFrame to store final results
final_df = pd.DataFrame()

# Group 'result_df' by dealership and calculate required statistics
grouped = result_df.groupby('dealerMarketingName')
final_df['dealerCd'] = grouped['dealerCd'].first()
final_df['dealerName'] = grouped['dealerName'].first()
final_df['phoneNumber'] = grouped['phoneNumber'].first()
final_df['webSite'] = grouped['webSite'].first()
final_df['postalAddress'] = grouped['postalAddress'].first()
final_df['cityName'] = grouped['cityName'].first()
final_df['state'] = grouped['state'].first()
final_df['zipCode'] = grouped['zipCode'].first()
final_df['number_of_cars_with_markup'] = grouped.size()
final_df['minimum_markup'] = grouped['markup'].min()
final_df['average_markup'] = grouped['markup'].mean()
final_df['maximum_markup'] = grouped['markup'].max()

# Reset the index
final_df.reset_index(level=0, inplace=True)

final_df.to_parquet(parquet_file_path)
final_df.to_excel(excel_file_path)
