# connects to PostgreSQL database using SQLAlchemy to create a table for the data and then to append the
# data from the dataframe to the SQL table

import pandas as pd
from sqlalchemy import create_engine, text
import os
from dotenv import load_dotenv

# loading the postgresql login details in .env
load_dotenv(override=True)
posgres_user = os.getenv("USER_NAME")
posgres_pass = os.getenv("PASSWORD")

# establish connections using psycopg2
conn_string = f'postgresql+psycopg2://{posgres_user}:{posgres_pass}@localhost:5432/polygon_stock_database'

engine = create_engine(conn_string)
conn = engine.connect()

# creating empy table with correct column names and data types
sql_table = '''CREATE TABLE daily_stock_data(ticker text, open numeric(10,2), high numeric(10,2), low numeric(10,2), close numeric(10,2), volume bigint, vwap numeric(10,2), timestamp bigint, transactions bigint, trading_date date, retrieved_date timestamp)'''
conn.execute(text(sql_table))
conn.commit()

# loading csv to datarfame
historic_df = pd.read_csv(r"C:\Users\jakes\OneDrive\Desktop\Github\Data\historic-trading-data\historic_data.csv")
# converting transactions to the correct data type
historic_df['transactions'] = historic_df['transactions'].astype('Int64')
# importing dataframe to sql table
historic_df.to_sql('daily_stock_data', conn, if_exists='append', index=False)
conn.commit()
