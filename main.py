import os
import io
import csv
from datetime import datetime, date, timedelta
from typing import Dict, Optional
from contextlib import contextmanager

import requests
import psycopg
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool
from fastapi import FastAPI, HTTPException, Query, Header
import logging

# Add after other imports
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="ECB FX Service", version="1.0.0")

# ---- Config ----
UPDATE_TOKEN = os.getenv("UPDATE_TOKEN")
ECB_CSV_URL = (
    "https://data-api.ecb.europa.eu/service/data/EXR/D..EUR.SP00.A"
    "?format=csvdata&lastNObservations=1"
)

# PostgreSQL connection configuration
# Option 1: Full connection string (highest priority)
DB_CONNECTION_STRING = os.getenv("DB_CONNECTION_STRING")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT", "5432")  # Default port is standard
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

# Option 2: Individual connection parameters
# For Cloud SQL: leave DB_HOST unset to use Unix socket, or set DB_CLOUD_SQL_INSTANCE
DB_HOST = os.getenv("DB_HOST")  # TCP connection host (IP or hostname)
DB_PORT = os.getenv("DB_PORT", "5432")  # Default PostgreSQL port
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

# Cloud SQL Unix socket configuration (used when DB_HOST is not set)
DB_CLOUD_SQL_INSTANCE = os.getenv("DB_CLOUD_SQL_INSTANCE")  # e.g., "openbookings:europe-west1:openbookings-db"

# Connection pool
_connection_pool: Optional[ConnectionPool] = None


def get_connection_pool():
    """Initialize and return a connection pool."""
    global _connection_pool
    if _connection_pool is None:
        if DB_CONNECTION_STRING:
            # Use full connection string if provided
            conn_string = DB_CONNECTION_STRING
        else:
            # Validate required parameters
            if not all([DB_NAME, DB_USER, DB_PASSWORD]):
                missing = [k for k, v in {
                    "DB_NAME": DB_NAME,
                    "DB_USER": DB_USER,
                    "DB_PASSWORD": DB_PASSWORD
                }.items() if not v]
                raise ValueError(
                    "Database configuration missing. Set DB_HOST, DB_NAME, DB_USER, DB_PASSWORD "
                    "or provide DB_CONNECTION_STRING environment variable"
                )
            
            # Determine connection method
            if DB_HOST:
                # TCP connection (standard PostgreSQL connection)
                conn_string = (
                    f"host={DB_HOST} port={DB_PORT} dbname={DB_NAME} "
                    f"user={DB_USER} password={DB_PASSWORD}"
                )
            elif DB_CLOUD_SQL_INSTANCE:
                # Cloud SQL Unix socket connection
                socket_path = f"/cloudsql/{DB_CLOUD_SQL_INSTANCE}"
                conn_string = (
                    f"host={socket_path} dbname={DB_NAME} "
                    f"user={DB_USER} password={DB_PASSWORD}"
                )
            else:
                raise ValueError(
                    "Database connection method not specified. "
                    "Set either DB_HOST (for TCP connection) or DB_CLOUD_SQL_INSTANCE "
                    "(for Cloud SQL Unix socket connection), or provide DB_CONNECTION_STRING."
                )
        
        logger.info("Initializing database connection pool...")
        _connection_pool = ConnectionPool(conn_string, min_size=1, max_size=10)
    return _connection_pool


@contextmanager
def get_db_connection():
    """Get a database connection from the pool."""
    pool = get_connection_pool()
    conn = pool.getconn()
    try:
        yield conn
    finally:
        pool.putconn(conn)

# ---- Helpers ----
def fetch_ecb_latest_csv() -> Dict:
    """
    Fetch latest daily FX reference rates from ECB as CSV.
    Returns:
      {
        "date": "YYYY-MM-DD",
        "base": "EUR",
        "rates": {"USD": 1.1721, ...},
        "source": "ECB"
      }
    """
    r = requests.get(ECB_CSV_URL, timeout=30)
    r.raise_for_status()

    # ECB CSV typically contains many columns; we only need:
    # TIME_PERIOD, CURRENCY, OBS_VALUE (sometimes also other dimensions)
    text = r.text
    reader = csv.DictReader(io.StringIO(text))

    rates: Dict[str, float] = {}
    dates: Dict[str, str] = {}  # Track date for each currency
    found_date: Optional[str] = None

    for row in reader:
        # Common column names in SDMX CSV exports:
        # - "TIME_PERIOD"
        # - "CURRENCY"
        # - "OBS_VALUE"
        date = row.get("TIME_PERIOD", "").strip() if row.get("TIME_PERIOD") else None
        ccy = row.get("CURRENCY", "").strip() if row.get("CURRENCY") else None
        val = row.get("OBS_VALUE", "").strip() if row.get("OBS_VALUE") else None

        if not date or not ccy or not val:
            continue

        # Track date for each currency individually
        dates[ccy] = date
        found_date = found_date or date  # Keep for backward compatibility

        try:
            rates[ccy] = float(val)
        except ValueError:
            continue
    
    # Log a sample of dates to verify they're being extracted correctly
    if dates:
        sample_items = list(dates.items())[:3]
        logger.info(f"Sample extracted dates: {sample_items}")

    if not found_date or not rates:
        raise RuntimeError("ECB response parsed but no rates found")

    return {
        "date": found_date,  # Keep for backward compatibility
        "base": "EUR",
        "rates": rates,
        "dates": dates,  # Add per-currency dates
        "source": "ECB",
    }


def store_snapshot(snapshot: Dict) -> None:
    """
    Store exchange rates snapshot in PostgreSQL.
    Inserts/updates rows in exchange_rates table for each currency.
    Uses the date specific to each currency if available, otherwise falls back to the snapshot date.
    Skips currencies with dates older than the current date.
    """
    default_date = snapshot["date"]
    rates = snapshot.get("rates", {})
    dates = snapshot.get("dates", {})  # Per-currency dates
    current_date = date.today() - timedelta(days=1)
    
    logger.info(f"Current date: {current_date}, Default date: {default_date}, Per-currency dates available: {len(dates)}")
    
    skipped_count = 0
    stored_count = 0

    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                # Use INSERT ... ON CONFLICT to upsert
                # Note: This assumes a UNIQUE constraint on (currency_code, date) in the exchange_rates table
                for currency_code, exchange_rate in rates.items():
                    # Use currency-specific date if available, otherwise use default date
                    currency_date_str = dates.get(currency_code, default_date)
                    if currency_code not in dates:
                        logger.debug(f"Currency {currency_code} not found in dates dict, using default date {default_date}")
                    
                    # Parse the date string and compare with current date
                    try:
                        currency_date = datetime.strptime(currency_date_str, "%Y-%m-%d").date()
                        # Skip if the currency date is older than current date
                        if currency_date < current_date:
                            skipped_count += 1
                            logger.debug(f"Skipping {currency_code} with date {currency_date_str} (older than current date {current_date})")
                            continue
                    except ValueError as e:
                        # If date parsing fails, log and skip this currency
                        logger.warning(f"Invalid date format for {currency_code}: {currency_date_str} - {e}")
                        skipped_count += 1
                        continue
                    
                    stored_count += 1
                    cur.execute(
                        """
                        INSERT INTO exchange_rates (currency_code, date, exchange_rate)
                        VALUES (%s, %s, %s)
                        ON CONFLICT (currency_code, date) 
                        DO UPDATE SET 
                            exchange_rate = EXCLUDED.exchange_rate
                        """,
                        (currency_code, currency_date_str, exchange_rate),
                    )
            conn.commit()
            logger.info(f"Stored {stored_count} exchange rates (skipped {skipped_count} with dates older than current date)")
    except Exception as e:
        logger.error(f"Error storing snapshot: {str(e)}", exc_info=True)
        raise


def load_snapshot(date: str) -> Optional[Dict]:
    """
    Load exchange rates snapshot for a specific date from PostgreSQL.
    Returns the snapshot in the same format as before.
    """
    with get_db_connection() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                """
                SELECT currency_code, exchange_rate, status, source
                FROM exchange_rates
                WHERE date = %s AND status = 'active'
                ORDER BY currency_code
                """,
                (date,),
            )
            rows = cur.fetchall()

    if not rows:
        return None

    # Reconstruct the snapshot format
    rates = {row["currency_code"]: float(row["exchange_rate"]) for row in rows}
    # Get source from first row (should be same for all)
    source = rows[0]["source"] if rows else "ECB"

    return {
        "date": date,
        "base": "EUR",
        "rates": rates,
        "source": source,
        "fetched_at": None,  # Not stored per-row, could add if needed
    }


def load_latest_snapshot() -> Optional[Dict]:
    """
    Load the latest exchange rates snapshot from PostgreSQL.
    Gets the most recent date and all currencies for that date.
    """
    with get_db_connection() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            # First, get the latest date
            cur.execute(
                """
                SELECT MAX(date) as latest_date
                FROM exchange_rates
                WHERE status = 'active'
                """
            )
            result = cur.fetchone()
            if not result or not result["latest_date"]:
                return None

            latest_date = result["latest_date"]

            # Then get all rates for that date
            cur.execute(
                """
                SELECT currency_code, exchange_rate, status, source
                FROM exchange_rates
                WHERE date = %s AND status = 'active'
                ORDER BY currency_code
                """,
                (latest_date,),
            )
            rows = cur.fetchall()

    if not rows:
        return None

    # Reconstruct the snapshot format
    rates = {row["currency_code"]: float(row["exchange_rate"]) for row in rows}
    source = rows[0]["source"] if rows else "ECB"

    # Convert date to string format (PostgreSQL may return date object)
    if isinstance(latest_date, datetime):
        date_str = latest_date.date().isoformat()
    elif hasattr(latest_date, "isoformat"):
        date_str = latest_date.isoformat()
    else:
        date_str = str(latest_date)

    return {
        "date": date_str,
        "base": "EUR",
        "rates": rates,
        "source": source,
        "fetched_at": None,  # Not stored per-row, could add if needed
    }


def convert_amount(amount: float, from_ccy: str, to_ccy: str, rates: Dict[str, float]) -> float:
    from_ccy = from_ccy.upper()
    to_ccy = to_ccy.upper()

    if amount < 0:
        raise ValueError("amount must be >= 0")

    if from_ccy == to_ccy:
        return amount

    # ECB base is EUR (implicit EUR rate = 1.0)
    def get_rate(ccy: str) -> float:
        if ccy == "EUR":
            return 1.0
        if ccy not in rates:
            raise KeyError(ccy)
        return float(rates[ccy])

    r_from = get_rate(from_ccy)
    r_to = get_rate(to_ccy)

    # X -> Y using EUR base:
    # amount_in_eur = amount / r_from  (unless from is EUR)
    # result = amount_in_eur * r_to
    # Combined: amount * (r_to / r_from)
    return amount * (r_to / r_from)


# ---- Endpoints ----
@app.get("/health")
def health():
    return {"ok": True}


@app.post("/tasks/update")
def update_rates(x_update_token: Optional[str] = Header(default=None)):
    """
    Called by Cloud Scheduler (or manually) to fetch and store the latest ECB snapshot.
    Protected by UPDATE_TOKEN environment variable (or Cloud Run IAM).
    """
    # Validate authentication token
    if not UPDATE_TOKEN:
        logger.warning("UPDATE_TOKEN not set - endpoint is unprotected")
    elif not x_update_token or x_update_token != UPDATE_TOKEN:
        raise HTTPException(status_code=401, detail="Unauthorized: Invalid or missing update token")

    try:
        logger.info("Fetching ECB rates...")
        snapshot = fetch_ecb_latest_csv()
        logger.info(f"Fetched snapshot for date: {snapshot['date']}, {len(snapshot['rates'])} rates")
        
        logger.info("Storing snapshot to database...")
        store_snapshot(snapshot)
        logger.info("Successfully stored snapshot")
        
        return {
            "stored": True,
            "date": snapshot["date"],
            "base": snapshot["base"],
            "count": len(snapshot["rates"])
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in update_rates: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Internal error: {str(e)}")

@app.get("/convert")
def convert(
    amount: float = Query(..., ge=0),
    from_ccy: str = Query(..., alias="from", min_length=3, max_length=3),
    to_ccy: str = Query(..., alias="to", min_length=3, max_length=3),
    date: Optional[str] = Query(None, description="YYYY-MM-DD; if omitted uses latest stored snapshot"),
    precision: int = Query(2, ge=0, le=12),
):
    snap = load_snapshot(date) if date else load_latest_snapshot()
    if not snap:
        raise HTTPException(status_code=404, detail="No rates available. Call /tasks/update first.")

    rates = snap.get("rates", {})

    try:
        result = convert_amount(amount, from_ccy, to_ccy, rates)
    except KeyError as e:
        currency = str(e).strip('"')
        raise HTTPException(status_code=400, detail=f"Unsupported currency: {currency}")
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    # Output rounding only at the end
    rounded = round(result, precision)

    # Effective rate (per 1 unit of from currency)
    try:
        eff_rate = convert_amount(1.0, from_ccy, to_ccy, rates)
    except Exception:
        eff_rate = None

    return {
        "amount": amount,
        "from": from_ccy.upper(),
        "to": to_ccy.upper(),
        "result": rounded,
        "raw_result": result,
        "rate": round(eff_rate, 12) if eff_rate is not None else None,
        "date": snap["date"],
        "base": snap.get("base", "EUR"),
        "source": snap.get("source", "ECB"),
        "fetched_at": snap.get("fetched_at"),
    }
