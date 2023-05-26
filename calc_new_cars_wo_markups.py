#!/usr/bin/python

import pandas as pd
import os
import re

base_dir = "/var/www/html/toyota"
parquet_file_name = "new_cars_wo_markups.parquet"
excel_file_name = "new_cars_wo_markups.xlsx"

# Get the list of all directories in the path, sorted in reverse order
dirs = [d for d in os.listdir(base_dir) if os.path.isdir(
    os.path.join(base_dir, d))]

# Filter out directories not matching the YYYY-MM-DD format
dirs = [d for d in dirs if re.match(r'\d{4}-\d{2}-\d{2}', d) is not None]

# Sort the directories in descending order
dirs.sort(reverse=True)

# Load the two most recent files
file1_path = os.path.join(base_dir, dirs[0], f'{dirs[0]}.parquet')
file2_path = os.path.join(base_dir, dirs[1], f'{dirs[1]}.parquet')

# Set output file paths
parquet_file_path = os.path.join(base_dir, dirs[0], parquet_file_name)
excel_file_path = os.path.join(base_dir, dirs[0], excel_file_name)

df_latest = pd.read_parquet(file1_path)
df_prev = pd.read_parquet(file2_path)

# Ensure vin fields are string
df_latest['vin'] = df_latest['vin'].astype(str)
df_prev['vin'] = df_prev['vin'].astype(str)

# Find new cars (those that are in the latest df but not in the previous one)
new_cars = df_latest[~df_latest['vin'].isin(df_prev['vin'])]

# Further filter for cars with a markup smaller than 100 and "isPreSold" is False
new_cars = new_cars[(new_cars['markup'] < 500)]

new_cars.to_parquet(parquet_file_path)
new_cars.to_excel(excel_file_path)
