#!/usr/bin/python

import requests
import json
from time import sleep
from random import randint
import datetime
import os


base_dir = "/var/www/html/toyota"
zipcodes = ["94123", "85001", "84044", "97035", "58854",
            "76006", "64030", "32401", "49601", "22901", "04401"]
# sf, phoenix, salt lake city, portland, watford city, fort worth, kansas city, panama city, cadillac, charlottesville, bangor
my_leadid = "5a92786c-531d-4fd2-9954-470044207fc6"
my_model = "sienna"


def wait():
    sleep(randint(12, 36))


def fetchvehicles(zipcode, seriescodes, leadid):
    url = "https://api.search-inventory.toyota.com/graphql"

    page_no = 1
    has_more_pages = True

    vehicles = []

    while has_more_pages:
        query = f"""
query {{
  locateVehiclesByZip(zipCode: "{zipcode}", brand: "TOYOTA", pageNo: {page_no}, pageSize: 250, seriesCodes: "{seriescodes}", distance: 500, leadid: "{leadid}") {{
    pagination {{
      pageNo
      pageSize
      totalPages
      totalRecords
    }}
    vehicleSummary {{
      vin
      stockNum
      brand
      marketingSeries
      year
      isTempVin
      dealerCd
      dealerCategory
      distributorCd
      holdStatus
      weightRating
      isPreSold
      dealerMarketingName
      dealerWebsite
      isSmartPath
      distance
      isUnlockPriceDealer
      transmission {{
        transmissionType
      }}
      price {{
        advertizedPrice
        nonSpAdvertizedPrice
        totalMsrp
        sellingPrice
        dph
        dioTotalMsrp
        dioTotalDealerSellingPrice
        dealerCashApplied
        baseMsrp
      }}
      options {{
        optionCd
        marketingName
        marketingLongName
        optionType
        packageInd
      }}
      mpg {{
        city
        highway
        combined
      }}
      model {{
        modelCd
        marketingName
        marketingTitle
      }}
      media {{
        type
        href
        imageTag
        source
      }}
      intColor {{
        colorCd
        colorSwatch
        marketingName
        nvsName
        colorFamilies
      }}
      extColor {{
        colorCd
        colorSwatch
        marketingName
        colorHexCd
        nvsName
        colorFamilies
      }}
      eta {{
        currFromDate
        currToDate
      }}
      engine {{
        engineCd
        name
      }}
      drivetrain {{
        code
        title
        bulletlist
      }}
      family
      cab {{
        code
        title
        bulletlist
      }}
      bed {{
        code
        title
        bulletlist
      }}
    }}
  }}
}}
"""

        headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36",
        }

        res = requests.post(url=url, json={"query": query}, headers=headers)
        # print("page ", page_no, ", status: ", res.status_code, " ", res.reason)
        if res.status_code != 200:
            print("error: [", res.status_code, "] ", res.reason)
            exit(1)

        data = res.json()

        vehicles += data['data']['locateVehiclesByZip']['vehicleSummary']

        page_no = data['data']['locateVehiclesByZip']['pagination']['pageNo']
        total_pages = data['data']['locateVehiclesByZip']['pagination']['totalPages']

        if page_no >= total_pages:
            has_more_pages = False
            # print("no more pages")
        else:
            page_no += 1

        wait()
    return vehicles


vehicles = {}
for zipcode in zipcodes:
    vehicles[zipcode] = fetchvehicles(
        zipcode, my_model, my_leadid)
    wait()

for zipcode in vehicles:
    date = datetime.datetime.now().date()
    jsonfilename = base_dir + "/" + \
        date.isoformat() + "/" + date.isoformat() + "-" + str(zipcode) + ".json"
    os.makedirs(os.path.dirname(jsonfilename), exist_ok=True)
    with open(jsonfilename, "w", encoding="UTF8") as f:
        json.dump(vehicles[zipcode], f, ensure_ascii=False, indent=4)
