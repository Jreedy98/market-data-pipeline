# fetches US stock trading data from polygon.io for the previous trading date from the current date
# the data is stored in a dataframe and then added to a postgreSQL table

import os
from dotenv import load_dotenv
from polygon import RESTClient
from datetime import datetime, timedelta
import holidays
import pandas as pd
from sys import exit
from sqlalchemy import create_engine, text

# loading the polygon api key stored in .env
load_dotenv(override=True)
poly_api_key = os.getenv("POLYGON_API_KEY")

# obtaining the previous trading day for the current date (excluding sunday and monday where the previous dates no trading occurs)
def previous_trading_date():
    if datetime.now().weekday() == 6 or datetime.now().weekday() == 0:
        print("yesterday was the weekend")
        return None  
    print("previous trading date set")
    get_previous_date = datetime.now() - timedelta(1)
    return get_previous_date

# checking if the previous date was a US bank holiday where trading did not occur
def holiday_checked_date():
    date_check = previous_trading_date()
    if date_check == None: 
        print("non-trading date")
        return None
    elif holidays.NYSE().get(date_check) != None:
        print("NYSE holiday yesterday")
        return None    
    print("previous trading day data collected")
    return date_check.strftime("%Y-%m-%d")

# accessing the stock summary data for the previous trading day
trading_date = holiday_checked_date()
if trading_date == None:
     print("exiting - no trading occured")
     exit()
client = RESTClient(api_key = poly_api_key)
try:
    daily_stock_data = client.get_grouped_daily_aggs(
        trading_date,
        adjusted="true",
        include_otc="true"
    )
except Exception as e:
    print(f"API error: {e}")
    exit()

# storing stock data to a dataframe
daily_stock_df = pd.DataFrame(daily_stock_data)

# adding metadeta columns
daily_stock_df['trading_date'] = trading_date
daily_stock_df['retrieved_date'] = datetime.now()

# standard cleaning - as done with the initial historci data that was loaded into the table
# dropping OTC column to maintain standards of table
daily_stock_df = daily_stock_df.drop(['otc'], axis='columns')
# dropping null tickers (if any)
daily_stock_df = daily_stock_df.dropna(subset=['ticker'])
# converting transactions to the correct data type
daily_stock_df['transactions'] = daily_stock_df['transactions'].astype('Int64')

# loading posgres login details from .env
posgres_user = os.getenv("USER_NAME")
posgres_pass = os.getenv("PASSWORD")

# establish connections using psycopg2
conn_string = f'postgresql+psycopg2://{posgres_user}:{posgres_pass}@localhost:5432/polygon_stock_database'

engine = create_engine(conn_string)
conn = engine.connect()

# querying database to check that data for that date has not already been loaded in the database
query = "SELECT COUNT(*) FROM daily_stock_data WHERE trading_date = :date"
q_result = conn.execute(text(query), {"date": trading_date})
# if data is already loaded for that date it will exit 
if q_result.fetchone()[0] > 0:
    print(f"exiting - trading data for {trading_date} has already been loaded!")
    exit()

# if data has not been loaded, it will try to append the daily stock dataframe to the exsisting table 
try:
    daily_stock_df.to_sql('daily_stock_data', conn, if_exists='append', index=False)
    conn.commit()
except Exception as e:
    print(f"database error: {e}")
    exit()

print(f"trading data for {trading_date} successfully added to database")



