import os
import pandas as pd
import requests
import json
from glob import glob
import re
from time import sleep
from random import randint

# Directory paths and regex pattern
base_dir = "/var/www/html/toyota"
dealers_dir = os.path.join(base_dir, "dealers")
regex = re.compile(r'\d{4}-\d{2}-\d{2}')


def wait():
    sleep(randint(5, 15))


# Get a list of all directories in YYYY-MM-DD format
dirpaths = sorted([dp for dp in glob(base_dir + "/*/")
                   if regex.fullmatch(os.path.basename(os.path.normpath(dp)))],
                  key=os.path.getmtime, reverse=True)

# Headers for API request
headers = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36",
    "Referer": "https://www.toyota.com/",
    "Referrer-Policy": "strict-origin-when-cross-origin"
}

# Go through each daily directory
for dirpath in dirpaths:
    date = os.path.basename(os.path.normpath(dirpath))
    only_cars_parquet_file_path = os.path.join(
        dirpath, f"{date}_only_cars.parquet")
    cars_with_dealers_parquet_file_path = os.path.join(
        dirpath, f"{date}_cars_with_dealers.parquet")

    # If the enriched file does not exist, check if dealer data needs scraping
    if not os.path.exists(cars_with_dealers_parquet_file_path):
        # Load the car data
        df = pd.read_parquet(only_cars_parquet_file_path)

        # For each unique dealerCd
        for dealer_cd in df['dealerCd'].unique():
            dealer_file = os.path.join(dealers_dir, f"{dealer_cd}.json")

            # If the dealer file does not exist, fetch it
            if not os.path.exists(dealer_file):
                # print("Fetching ", dealer_cd)
                response = requests.get(
                    f"https://api.dg.toyota.com/api/v2/dealers/{dealer_cd}?brand=toyota", headers=headers)

                # Write the JSON response to a file
                with open(dealer_file, 'w', encoding="UTF8") as f:
                    json.dump(response.json(), f, ensure_ascii=False, indent=4)
                wait()
