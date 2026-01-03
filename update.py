import requests
import csv
from datetime import datetime, timedelta

def fetch_ecb_csv():
    ECB_CSV_URL = "https://data-api.ecb.europa.eu/service/data/EXR/D..EUR.SP00.A?format=csvdata&lastNObservations=1"
    response = requests.get(ECB_CSV_URL)
    response.raise_for_status()
    csv_data = response.text
    csv_reader = csv.DictReader(csv_data.splitlines())
    return csv_reader

def prepare_data_for_database(csv_reader):
    data = []
    cutoff_date = (datetime.now() - timedelta(days=1)).date()
    
    for row in csv_reader:
        # Map CSV columns to our fields
        currency_code = row.get("CURRENCY")
        exchange_rate = row.get("OBS_VALUE")
        time_period = row.get("TIME_PERIOD")
        
        # Skip rows with missing required fields
        if not all([currency_code, exchange_rate, time_period]):
            continue
        
        # Parse the date and filter
        try:
            date_obj = datetime.strptime(time_period, "%Y-%m-%d").date()
            if date_obj < cutoff_date:
                continue
        except ValueError:
            # Skip rows with invalid date format
            continue
        
        data.append((currency_code, time_period, exchange_rate))
    return data

# GET SQL INSERT STATEMENT
sql_insert = """
INSERT INTO exchange_rates (currency_code, date, exchange_rate)
VALUES (%s, %s, %s)
ON CONFLICT (currency_code, date)
DO UPDATE SET
exchange_rate = EXCLUDED.exchange_rate
"""

def insert_data_into_database(data, conn):
    with conn.cursor() as cursor:
        cursor.executemany(sql_insert, data)
        conn.commit()