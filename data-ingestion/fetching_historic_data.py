# fetches the daily US stock summary trading data for the days where trading occured between 29-04-25 to  30-10-25 from polygon.io
# each days data is stored in a dataframe, added to a list, then concatanated to one dataframe, and finally saved as a csv

import os
from dotenv import load_dotenv
from polygon import RESTClient
import pandas as pd
import holidays
from datetime import datetime, timedelta
from time import sleep
from sys import exit
from tqdm import tqdm

# loading the polygon api key stored in .env
load_dotenv(override=True)
poly_api_key = os.getenv("POLYGON_API_KEY")

# function to obtain list of valid trading days for 6 month time period
def historic_data():
    start_date = datetime(2025, 4, 29)
    end_date = datetime(2025, 10, 30)
    delta = timedelta(days=1)
    valid_date = []
    while start_date < end_date:
        if start_date.weekday() == 5 or start_date.weekday() == 6:
            print(f"{start_date} was a weekend")
            start_date += delta
            continue
        elif holidays.NYSE().get(start_date) != None:
            print(f"{start_date} was a NYSE holiday")
            start_date += delta
            continue
        else:
            valid_date.append(start_date.strftime("%Y-%m-%d"))
            start_date += delta
    return valid_date

# accessing the stock summary data for the histroical dates that were not a weekend or NYSE holiday
trading_dates = historic_data()
all_dfs = []
client = RESTClient(api_key = poly_api_key)

for date in tqdm(trading_dates, desc="obtaining historic stock data"):
    date_stock_data = client.get_grouped_daily_aggs(
        date,
        adjusted="true",
        include_otc="true"
    )
    day_df = pd.DataFrame(date_stock_data)
    day_df['trading_date'] = date
    day_df['retrieved_date'] = datetime.now()
    all_dfs.append(day_df)
    sleep(20)

#appending the list of stock market df's to one dataframe and saving to csv
historic_df = pd.concat(all_dfs, ignore_index = True)
historic_df.to_csv('historic_data.csv', index=False)
print("historic trading data now saved")
