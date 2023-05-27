import os
import pandas as pd
import re
from pathlib import Path
import pyarrow.parquet as pq
from openpyxl import Workbook

# regex for date pattern in the directory name
date_pattern = re.compile(r'\d{4}-\d{2}-\d{2}')

# directory containing daily directories and dealers data
base_dir = Path('/var/www/html/toyota')

# path to dealers data
dealer_data_path = base_dir / 'dealers' / 'dealers.parquet'

# read dealers data into a dataframe
dealer_df = pd.read_parquet(dealer_data_path)

for subdir in base_dir.iterdir():
    # check if the subdir matches the date pattern and is a directory
    if date_pattern.fullmatch(subdir.name) and subdir.is_dir():
        only_cars_file = subdir / f'{subdir.name}_only_cars.parquet'
        cars_with_dealers_file = subdir / \
            f'{subdir.name}_cars_with_dealers.parquet'
        cars_with_dealers_excel = subdir / \
            f'{subdir.name}_cars_with_dealers.xlsx'

        # check if the input file exists and the output file doesn't
        if only_cars_file.is_file() and not cars_with_dealers_file.is_file():
            # read cars data into a dataframe
            car_df = pd.read_parquet(only_cars_file)

            # merge cars and dealers data
            merged_df = car_df.merge(dealer_df, on='dealerCd', how='inner')

            # check if there's any car with missing dealer info
            if merged_df.shape[0] != car_df.shape[0]:
                raise ValueError(
                    f"Dealer info missing for one or more cars in {only_cars_file}")

            # write the merged data back into a parquet file
            merged_df.to_parquet(cars_with_dealers_file, engine='pyarrow')

            merged_df.to_excel(cars_with_dealers_excel,
                               engine='openpyxl', index=False)
