#!/usr/bin/python

import os
import pandas as pd
from pathlib import Path

base_dir = "/var/www/html/toyota"


# Loop over all directories in base_dir
for dir_path, dir_names, file_names in os.walk(base_dir):
    # The Parquet file name will be derived from directory name (which represents date)
    parquet_file_name = dir_path.split("/")[-1] + ".parquet"
    parquet_file_path = os.path.join(dir_path, parquet_file_name)

    csv_file_name = dir_path.split("/")[-1] + ".xlsx"
    csv_file_path = os.path.join(dir_path, csv_file_name)

    # If Parquet file does not exist, process the JSON files, else skip
    if not Path(parquet_file_path).is_file():
        # Accumulate DataFrame here
        df_list = []

        # Loop over all files in dir_path
        for file_name in file_names:
            # Check if the file is a JSON file
            if file_name.endswith(".json"):
                file_path = os.path.join(dir_path, file_name)
                # Read the JSON file into a DataFrame and append it to the list
                df = pd.read_json(file_path)

                ### TRANSFORMATIONS ###

                # Drop the 'media' and 'distance' columns
                df = df.drop(columns=['media', 'distance', 'family'])

                # Normalize the data
                df = pd.json_normalize(df.to_dict(
                    orient='records'), max_level=False)

                df = df.join(pd.json_normalize(
                    df.transmission).add_prefix("transmission."))  # type: ignore
                df = df.join(pd.json_normalize(
                    df.price).add_prefix("price."))  # type: ignore
                df = df.join(pd.json_normalize(
                    df.mpg).add_prefix("mpg."))  # type: ignore
                df = df.join(pd.json_normalize(
                    df.model).add_prefix("model."))  # type: ignore
                df = df.join(pd.json_normalize(
                    df.intColor).add_prefix("intColor."))  # type: ignore
                df = df.join(pd.json_normalize(
                    df.extColor).add_prefix("extColor."))  # type: ignore
                df = df.join(pd.json_normalize(
                    df.eta).add_prefix("eta."))  # type: ignore
                df = df.join(pd.json_normalize(
                    df.engine).add_prefix("engine."))  # type: ignore
                df = df.join(pd.json_normalize(
                    df.drivetrain).add_prefix("drivetrain."))  # type: ignore

                # Flatten options (list of dicts)
                if 'options' in df.columns:
                    df['options'] = df['options'].apply(lambda x: ', '.join(
                        [d.get('marketingName', '') for d in x if d.get('marketingName') is not None]) if isinstance(x, list) else x)

                # Drop the original nested columns
                df = df.drop(columns=["transmission", "price", "mpg", "model", "intColor", "extColor", "eta",
                             "engine", "drivetrain", "intColor.colorFamilies", "extColor.colorFamilies"])

                # Calculate markup column
                df['markup'] = df['price.advertizedPrice'] - \
                    df['price.totalMsrp']

                # Old code to check for unnested data structures
                # dict_columns = [col for col in df.columns if df[col].apply(
                #    lambda x: isinstance(x, dict)).any()]
                # print("dicts:" + str(dict_columns))

                # list_columns = [col for col in df.columns if df[col].apply(
                #    lambda x: isinstance(x, list)).any()] # print("lists:" + str(list_columns))

                df_list.append(df)

        # Concatenate all DataFrames in the list, if it's not empty
        if df_list:
            df_final = pd.concat(df_list, ignore_index=True)

            df_final = df_final.drop_duplicates()

            # Write the final DataFrame to a Parquet file in the same directory
            df_final.to_parquet(parquet_file_path)
            df_final.to_excel(csv_file_path)
