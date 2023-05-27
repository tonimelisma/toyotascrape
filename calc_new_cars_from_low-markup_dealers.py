import pandas as pd
import os
import re
from pathlib import Path

base_dir = Path("/var/www/html/toyota")

# Create a regex for date pattern
date_pattern = re.compile(r'\d{4}-\d{2}-\d{2}')

# Get a list of all directories that match the date pattern
dirs = sorted([d for d in os.listdir(base_dir) if os.path.isdir(os.path.join(
    base_dir, d)) and date_pattern.fullmatch(d) is not None], reverse=True)

# Get the directory of the latest date
latest_dir = base_dir / dirs[0]

# Get the dealer markups file for the latest date
dealer_markups_file = latest_dir / f"{dirs[0]}_dealer_markups.parquet"

# Load the dealer markups DataFrame
df_dealer_markups = pd.read_parquet(dealer_markups_file)

# Get dealers with average markup of less than 500
low_markup_dealers = df_dealer_markups[df_dealer_markups['average_markup']
                                       < 500]['dealerMarketingName']

# Get the paths for today's and yesterday's cars with dealers files
cars_with_dealers_file_today = latest_dir / \
    f"{dirs[0]}_cars_with_dealers.parquet"
cars_with_dealers_file_yesterday = base_dir / \
    dirs[1] / f"{dirs[1]}_cars_with_dealers.parquet"

# Load the cars with dealers DataFrames
df_cars_with_dealers_today = pd.read_parquet(cars_with_dealers_file_today)
df_cars_with_dealers_yesterday = pd.read_parquet(
    cars_with_dealers_file_yesterday)

# Filter both DataFrames for only the cars from low markup dealers
df_cars_with_dealers_today = df_cars_with_dealers_today[df_cars_with_dealers_today['dealerMarketingName'].isin(
    low_markup_dealers)]
df_cars_with_dealers_yesterday = df_cars_with_dealers_yesterday[df_cars_with_dealers_yesterday['dealerMarketingName'].isin(
    low_markup_dealers)]

# Find new cars (those that are in today's DataFrame but not in yesterday's)
new_cars = df_cars_with_dealers_today[~df_cars_with_dealers_today['vin'].isin(
    df_cars_with_dealers_yesterday['vin'])]

# Save the new cars DataFrame to a Parquet file
new_cars_file_parquet = latest_dir / \
    f"{dirs[0]}_new_cars_from_low_markup_dealers.parquet"
new_cars_file_excel = latest_dir / \
    f"{dirs[0]}_new_cars_from_low_markup_dealers.xlsx"
new_cars.to_parquet(new_cars_file_parquet)
new_cars.to_excel(new_cars_file_excel)
