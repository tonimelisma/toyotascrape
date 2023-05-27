#!/usr/bin/python3

import os
import pandas as pd
import json

# Directory path
dealers_dir = "/var/www/html/toyota/dealers"

# Get a list of all JSON files in the directory
json_files = [f for f in os.listdir(dealers_dir) if f.endswith('.json')]

# Placeholder for data to be written to Parquet file
data = []

# Parquet file path
parquet_file = os.path.join(dealers_dir, 'dealers.parquet')

# If parquet file exists, load it into a dataframe
if os.path.isfile(parquet_file):
    df_existing = pd.read_parquet(parquet_file)

    # Get list of dealerCd from the existing dataframe
    existing_dealerCd = df_existing['dealerCd'].tolist()
else:
    df_existing = pd.DataFrame()
    existing_dealerCd = []

# Go through each JSON file
for json_file in json_files:
    dealer_cd = json_file.split('.')[0]  # Get the dealerCd from filename

    # If dealerCd already exists in the parquet file, skip this file
    if dealer_cd in existing_dealerCd:
        continue

    print("Adding dealer", dealer_cd)

    with open(os.path.join(dealers_dir, json_file), 'r') as f:
        dealer_data = json.load(f)

        # Get relevant data from the JSON file
        dealer_info = dealer_data['showDealerLocatorDataArea']['dealerLocator'][
            0]['dealerLocatorDetail'][0]['dealerParty']['specifiedOrganization']

        dealer_name = dealer_info['companyName']['value']
        primary_contact = dealer_info['primaryContact'][0]

        phone_number = next((item['completeNumber']['value']
                            for item in primary_contact['telephoneCommunication'] if item['channelCode']['value'] == 'Phone'), None)
        web_site = next((item['uriid']['value'] for item in primary_contact['uricommunication']
                        if item['channelCode']['value'] == 'Website'), None)

        postal_address = primary_contact['postalAddress']['lineOne']['value']
        city_name = primary_contact['postalAddress']['cityName']['value']
        state = primary_contact['postalAddress']['stateOrProvinceCountrySubDivisionID']['value']
        zip_code = primary_contact['postalAddress']['postcode']['value']

        # Append the data to the list
        data.append([dealer_cd, dealer_name, phone_number, web_site,
                    postal_address, city_name, state, zip_code])

# Convert the list to a DataFrame
df_new = pd.DataFrame(data, columns=['dealerCd', 'dealerName', 'phoneNumber',
                      'webSite', 'postalAddress', 'cityName', 'state', 'zipCode'])

# Append the new data to the existing data
df_combined = pd.concat([df_existing, df_new], ignore_index=True)

# Write the combined DataFrame to a Parquet file
df_combined.to_parquet(parquet_file, index=False)
