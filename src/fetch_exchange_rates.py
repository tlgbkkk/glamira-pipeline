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
    FROM `{PROJECT_ID}.raw_layer.summary_raw`
"""
date_result = list(client.query(query_date).result())[0]
min_date = date_result.min_date
max_date = date_result.max_date
print(f"-> Time range: {min_date} to {max_date}")

query_currency = f"""
    WITH all_currency AS (
    
        -- currency ở level order
        SELECT
            currency,
            current_url
        FROM `{PROJECT_ID}.raw_layer.summary_raw`
        WHERE currency IS NOT NULL
          AND TRIM(currency) != ''
          AND collection = 'checkout_success'
    
        UNION ALL
    
        -- currency trong cart_products
        SELECT
            cp.currency,
            sr.current_url
        FROM `{PROJECT_ID}.raw_layer.summary_raw` sr,
        UNNEST(sr.cart_products) cp
        WHERE cp.currency IS NOT NULL
          AND TRIM(cp.currency) != ''
          AND sr.collection = 'checkout_success'
    )
    
    SELECT DISTINCT currency
    FROM (
        SELECT
            CASE
    
                -- direct ISO
                WHEN UPPER(TRIM(currency)) IN (
                    'AED','AFN','ALL','ARS','AUD','AZN','BGN','BOB',
                    'BRL','CAD','CHF','CLP','CNY','COP','CRC','CZK',
                    'DKK','DOP','EUR','GBP','GTQ','HKD','HNL','HRK',
                    'HUF','IDR','INR','ISK','JPY','KRW','KWD','MDL',
                    'MXN','MYR','NOK','NZD','PEN','PHP','PLN','PYG',
                    'RON','RSD','SEK','SGD','TRY','TWD','USD','UYU',
                    'VND','ZAR'
                )
                THEN UPPER(TRIM(currency))
    
                -- symbols
                WHEN currency = '€'         THEN 'EUR'
                WHEN currency = '£'         THEN 'GBP'
                WHEN currency = '￥'        THEN 'JPY'
                WHEN currency = '₹'         THEN 'INR'
                WHEN currency = '₫'         THEN 'VND'
                WHEN currency = '₱'         THEN 'PHP'
                WHEN currency = '₲'         THEN 'PYG'
                WHEN currency = '₺'         THEN 'TRY'
                WHEN currency = '₩'         THEN 'KRW'
    
                -- local symbols
                WHEN currency = 'zł'        THEN 'PLN'
                WHEN currency = 'Kč'        THEN 'CZK'
                WHEN currency = 'Ft'        THEN 'HUF'
                WHEN currency = 'Lei'       THEN 'RON'
                WHEN currency = 'lei'       THEN 'RON'
                WHEN currency = 'din'       THEN 'RSD'
                WHEN currency = ' din.'     THEN 'RSD'
                WHEN currency = 'лв.'       THEN 'BGN'
                WHEN currency = 'kn'        THEN 'HRK'
                WHEN currency = 'د.ك.‏'     THEN 'KWD'
    
                -- explicit labels
                WHEN currency = 'R$'        THEN 'BRL'
                WHEN currency = 'CHF'       THEN 'CHF'
                WHEN currency = 'NZD $'     THEN 'NZD'
                WHEN currency = 'AU $'      THEN 'AUD'
                WHEN currency = 'AUD $'     THEN 'AUD'
                WHEN currency = 'HKD $'     THEN 'HKD'
                WHEN currency = 'SGD $'     THEN 'SGD'
                WHEN currency = 'CAD $'     THEN 'CAD'
                WHEN currency = 'MXN $'     THEN 'MXN'
                WHEN currency = 'CLP'       THEN 'CLP'
                WHEN currency = 'COP $'     THEN 'COP'
                WHEN currency = 'PEN S/.'   THEN 'PEN'
                WHEN currency = 'GTQ Q'     THEN 'GTQ'
                WHEN currency = 'CRC ₡'     THEN 'CRC'
                WHEN currency = 'USD $'     THEN 'USD'
                WHEN currency = 'BOB Bs'    THEN 'BOB'
                WHEN currency = 'BOB BS'    THEN 'BOB'
                WHEN currency = 'DOP $'     THEN 'DOP'
                WHEN currency = 'NT$'       THEN 'TWD'
                WHEN currency = 'RM'        THEN 'MYR'
                WHEN currency = 'Rp'        THEN 'IDR'
                WHEN currency = 'UYU'       THEN 'UYU'
    
                -- kr
                WHEN currency = 'kr'
                     AND current_url LIKE '%glamira.dk%' THEN 'DKK'
    
                WHEN currency = 'kr'
                     AND current_url LIKE '%glamira.no%' THEN 'NOK'
    
                WHEN currency = 'kr'
                     AND (
                        current_url LIKE '%glamira.se%'
                        OR current_url LIKE '%GLAMIRA.se%'
                     ) THEN 'SEK'
    
                WHEN currency = 'kr'
                     AND current_url LIKE '%glamira.is%' THEN 'ISK'
    
                -- $
                WHEN currency = '$'
                     AND current_url LIKE '%glamira.com.ar%' THEN 'ARS'
    
                WHEN currency = '$'
                     AND current_url LIKE '%glamira.com.au%' THEN 'AUD'
    
                WHEN currency = '$'
                     AND current_url LIKE '%glamira.ca%' THEN 'CAD'
    
                WHEN currency = '$'
                     AND current_url LIKE '%glamira.nz%' THEN 'NZD'
    
                WHEN currency = '$'
                     AND current_url LIKE '%glamira.sg%' THEN 'SGD'
    
                WHEN currency = '$'
                     AND current_url LIKE '%glamira.hk%' THEN 'HKD'
    
                WHEN currency = '$' THEN 'USD'
    
                ELSE NULL
    
            END AS currency
        FROM all_currency
    )
    WHERE currency IS NOT NULL
    ORDER BY currency
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