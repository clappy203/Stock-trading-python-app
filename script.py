import requests
import os
from dotenv import load_dotenv
load_dotenv()
from datetime import date
import snowflake.connector


POLYGON_API_KEY = os.getenv("POLYGON_API_KEY")
LIMIT = 1000

url = f'https://api.polygon.io/v3/reference/tickers?market=stocks&active=true&order=asc&limit={LIMIT}&sort=ticker&apiKey={POLYGON_API_KEY}'
response = requests.get(url)


example_ticker = {'ticker': 'A', 
'name': 'Agilent Technologies Inc.', 
'market': 'stocks', 
'locale': 'us', 
'primary_exchange': 'XNYS', 
'type': 'CS', 
'active': True, 
'currency_name': 'usd', 
'cik': '0001090872', 
'composite_figi': 'BBG000C2V3D6', 
'share_class_figi': 'BBG001SCTQY4', 
'last_updated_utc': '2025-10-15T06:05:51.037116752Z',
'ds': '2025-10-15'}


def get_ds_from_ticker(t):
    """Return date (YYYY-MM-DD) derived from last_updated_utc if present, else today's date."""
    lu = t.get('last_updated_utc')
    if lu:
        try:
            return lu.split('T')[0]
        except Exception:
            pass
    return date.today().isoformat()

# tickers to store full ticker dicts
tickers = []

data = response.json()
for ticker in data['results']:
    ticker['ds'] = get_ds_from_ticker(ticker)
    tickers.append(ticker)  # Store the full dict

# code for pagination...
while 'next_url' in data:
    print('requesting next page', data['next_url'])
    next_url = data['next_url']
    response = requests.get(next_url + f'&apikey={POLYGON_API_KEY}')
    data = response.json()
    if 'results' in data:
        for ticker in data['results']:
            ticker['ds'] = get_ds_from_ticker(ticker)
            tickers.append(ticker)  # Store the full dict
    else:
        print("No 'results' in response:", data)
    print(len(tickers))

# Prepare fieldnames (call to load performed in main guard below)
fieldnames = list(example_ticker.keys())

def run_stock_job():
    print(f"Loaded {len(tickers)} tickers to snowflake")

def load_to_snowflake(rows, fieldnames):
    # Build connection kwargs from environment variables
    connect_kwargs = {
        'user': os.getenv("SNOWFLAKE_USER"),
        'password': os.getenv("SNOWFLAKE_PASSWORD"),
    }
    account = os.getenv("SNOWFLAKE_ACCOUNT")
    if account:
        connect_kwargs['account'] = account
    else:
        print("Note: SNOWFLAKE_ACCOUNT not set. If connection fails, set SNOWFLAKE_ACCOUNT in your .env (use value from your Snowflake URL).")
    warehouse = os.getenv("SNOWFLAKE_WAREHOUSE")
    database = os.getenv("SNOWFLAKE_DATABASE")
    schema = os.getenv("SNOWFLAKE_SCHEMA")
    role = os.getenv("SNOWFLAKE_ROLE")
    if warehouse:
        connect_kwargs['warehouse'] = warehouse
    if database:
        connect_kwargs['database'] = database
    if schema:
        connect_kwargs['schema'] = schema
    if role:
        connect_kwargs['role'] = role

    ctx = snowflake.connector.connect(**connect_kwargs)     
    cs = ctx.cursor()
    try:
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS STOCK_TICKERS (
            ticker VARCHAR,
            name VARCHAR,
            market VARCHAR,
            locale VARCHAR,
            primary_exchange VARCHAR,
            type VARCHAR,
            active BOOLEAN,
            currency_name VARCHAR,
            cik VARCHAR,
            composite_figi VARCHAR,
            share_class_figi VARCHAR,
            last_updated_utc TIMESTAMP_NTZ,
            ds DATE
        );
        """
        cs.execute(create_table_query)
        insert_query = f"INSERT INTO STOCK_TICKERS ({', '.join(n.upper() for n in fieldnames)}) VALUES ({', '.join(['%s'] * len(fieldnames))})"
        data_to_insert = []
        for row in rows:
            data_to_insert.append([row.get(field, None) for field in fieldnames])
        try:
            cs.executemany(insert_query, data_to_insert)
            print(f"Inserted {len(rows)} rows into STOCK_TICKERS table.")
        except Exception as e:
            print("Snowflake insert failed:", e)
            print("Query:", insert_query)
            print("First row sample:", data_to_insert[:3])
            raise
    finally:
        cs.close()
        ctx.close()


if __name__ == "__main__":
    # Ensure password is present
    if not os.getenv("SNOWFLAKE_PASSWORD"):
        print("Error: SNOWFLAKE_PASSWORD not set in environment (.env). Please set SNOWFLAKE_PASSWORD in your .env file and rerun.")
    else:
        print(f"Loading {len(tickers)} tickers into Snowflake table STOCK_TICKERS")
        load_to_snowflake(tickers, fieldnames)
        run_stock_job()
