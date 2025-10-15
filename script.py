import requests
import os
from dotenv import load_dotenv
load_dotenv()
import csv


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
'last_updated_utc': '2025-10-15T06:05:51.037116752Z'}


# tickers to store full ticker dicts
tickers = []

data = response.json()
for ticker in data['results']:
    tickers.append(ticker)  # Store the full dict

# code for pagination...
while 'next_url' in data:
    print('requesting next page', data['next_url'])
    next_url = data['next_url']
    response = requests.get(next_url + f'&apikey={POLYGON_API_KEY}')
    data = response.json()
    if 'results' in data:
        for ticker in data['results']:
            tickers.append(ticker)  # Store the full dict
    else:
        print("No 'results' in response:", data)
    print(len(tickers))

# Write tickers to csv with example_ticker schema
fieldnames = list(example_ticker.keys())
output_csv = 'tickers.csv'
with open(output_csv, mode ='w', newline='', encoding='utf-8') as f:
    writer = csv.DictWriter(f, fieldnames=fieldnames)
    writer.writeheader()
    for t in tickers:  # Use the tickers list
        print(t)
        row = {key: t.get(key, '') for key in fieldnames}
        writer.writerow(row)
print(f'Wrote {len(tickers)} rows to {output_csv}')