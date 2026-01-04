import logging
import requests
import csv
from datetime import datetime, timedelta

# Set up logging
logger = logging.getLogger(__name__)

def fetch_ecb_csv():
    """Fetch exchange rate data from ECB API."""
    ECB_CSV_URL = "https://data-api.ecb.europa.eu/service/data/EXR/D..EUR.SP00.A?format=csvdata&lastNObservations=1"
    logger.info(f"Fetching exchange rates from ECB API: {ECB_CSV_URL}")
    try:
        response = requests.get(ECB_CSV_URL, timeout=30)
        response.raise_for_status()
        csv_data = response.text
        csv_reader = csv.DictReader(csv_data.splitlines())
        logger.info("Successfully fetched exchange rate data from ECB API")
        return csv_reader
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to fetch ECB data: {e}", exc_info=True)
        raise

def prepare_data_for_database(csv_reader):
    """Prepare CSV data for database insertion."""
    data = []
    cutoff_date = (datetime.now() - timedelta(days=1)).date()
    skipped_count = 0
    
    logger.debug(f"Preparing data with cutoff date: {cutoff_date}")
    
    for row in csv_reader:
        # Map CSV columns to our fields
        currency_code = row.get("CURRENCY")
        exchange_rate = row.get("OBS_VALUE")
        time_period = row.get("TIME_PERIOD")
        
        # Skip rows with missing required fields
        if not all([currency_code, exchange_rate, time_period]):
            skipped_count += 1
            logger.debug(f"Skipping row with missing fields: {row}")
            continue
        
        # Parse the date and filter
        try:
            date_obj = datetime.strptime(time_period, "%Y-%m-%d").date()
            if date_obj < cutoff_date:
                skipped_count += 1
                logger.debug(f"Skipping row with old date: {time_period}")
                continue
        except ValueError:
            # Skip rows with invalid date format
            skipped_count += 1
            logger.debug(f"Skipping row with invalid date format: {time_period}")
            continue
        
        data.append((currency_code, time_period, exchange_rate))
    
    logger.info(f"Prepared {len(data)} records for insertion (skipped {skipped_count} records)")
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
    """Insert exchange rate data into the database."""
    if not data:
        logger.warning("No data provided for insertion")
        return
    
    logger.info(f"Inserting {len(data)} exchange rate records into database")
    cursor = conn.cursor()
    try:
        cursor.executemany(sql_insert, data)
        logger.info(f"Successfully inserted {len(data)} exchange rate records")
    except Exception as e:
        logger.error(f"Failed to insert data: {e}", exc_info=True)
        raise
    finally:
        cursor.close()

# GET SQL SELECT STATEMENT
sql_select = """
SELECT * FROM exchange_rates
"""

def test_db_connection(conn):
    """Test database connection by fetching all exchange rates."""
    logger.debug("Testing database connection")
    cursor = conn.cursor()
    try:
        cursor.execute(sql_select)
        result = cursor.fetchall()
        logger.debug(f"Database test query returned {len(result)} rows")
        return result
    except Exception as e:
        logger.error(f"Database test query failed: {e}", exc_info=True)
        raise
    finally:
        cursor.close()