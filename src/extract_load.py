import yfinance as yf
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

load_dotenv()

commodities = ['CL=F', 'CG=F', 'SI=F']

DB_HOST = os.getenv('DB_HOST_PROD')
DB_PORT = os.getenv('DB_PORT_PROD')
DB_NAME = os.getenv('DB_NAME_PROD')
DB_USER = os.getenv('DB_USER_PROD')
DB_PASS = os.getenv('DB_PASS_PROD')
DB_SCHEMA =os.getenv('DB_SCHEMA_PROD')

DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

engine = create_engine(DATABASE_URL)

def get_data_commodities(symbol, period='5d', interval='1d'):
    ticker = yf.Ticker('CL=F')
    data = ticker.history(period = period, interval = interval)[['Close']]
    data['symbol'] = symbol
    return data

def get_all_data_commodities(commodities):
    all_data = []
    for symbol in commodities:
        data = get_data_commodities(symbol)
        all_data.append(data)
    return pd.concat(all_data)

def save_postgres(df, schema='public'):
    df.to_sql('commodities', engine, if_exists='replace', index=True, index_label='Date', schema=schema)

if __name__ == "__main__":
    data_concat = get_all_data_commodities(commodities)
    save_postgres(data_concat, schema='public')