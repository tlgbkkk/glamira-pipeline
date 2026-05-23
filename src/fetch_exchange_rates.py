import requests
import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta
from google.cloud import bigquery
from src.config import PROJECT_ID

client = bigquery.Client(project=PROJECT_ID)

print("Reading data from BigQuery...")

query_date = f"""
    SELECT 
        EXTRACT(DATE FROM MIN(TIMESTAMP_SECONDS(time_stamp))) as min_date,
        EXTRACT(DATE FROM MAX(TIMESTAMP_SECONDS(time_stamp))) as max_date
    FROM `{PROJECT_ID}.glamira_raw.summary`
"""
date_result = list(client.query(query_date).result())[0]
min_date = date_result.min_date
max_date = date_result.max_date
print(f"-> Time range: {min_date} to {max_date}")

query_currency = f"""
    SELECT DISTINCT UPPER(TRIM(currency)) as currency
    FROM `{PROJECT_ID}.glamira_raw.product_dictionary`
    WHERE currency IS NOT NULL 
      AND TRIM(currency) != ''
"""
currency_rows = client.query(query_currency).result()

currencies_to_fetch = [row.currency for row in currency_rows if row.currency != 'USD']
print(f"-> Currencies need to fetch: {currencies_to_fetch}")

if not currencies_to_fetch:
    print("Found no currency except USD.")
    exit()

current_date = datetime(min_date.year, min_date.month, 1).date()
target_dates = []

while current_date <= max_date:
    target_dates.append(current_date)
    current_date += relativedelta(months=1)

print("\nCrawling data by Frankfurter API...")
records = []
quotes_param = ','.join(currencies_to_fetch)

for dt in target_dates:
    date_str = dt.strftime('%Y-%m-%d')
    url = f"https://api.frankfurter.dev/v2/rates?date={date_str}&base=USD&quotes={quotes_param}"

    try:
        response = requests.get(url).json()

        for item in response:
            quote_currency = item['quote']
            rate_val = item['rate']

            to_usd_rate = 1 / rate_val

            records.append({
                "month": dt.month,
                "year": dt.year,
                "currency": quote_currency,
                "to_usd_rate": round(to_usd_rate, 8)
            })

        records.append({
            "month": dt.month,
            "year": dt.year,
            "currency": "USD",
            "to_usd_rate": 1.0
        })

        print(f"Fetched rate {dt.month}/{dt.year}")

    except Exception as e:
        print(f"Error when getting date {date_str}: {e}")

print("\nExporting data...")
df = pd.DataFrame(records)
output_path = "glamira_dbt/seeds/currency_exchange.csv"
df.to_csv(output_path, index=False)

print(f"DONE! Saved {len(df)} rate lines into {output_path}")