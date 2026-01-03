import os
import psycopg2
from psycopg2 import pool
from contextlib import contextmanager

# ---- Configuration ----

INSTANCE_CONNECTION_NAME = os.getenv(
    "INSTANCE_CONNECTION_NAME",
    "openbookings:europe-west1:openbookings-db",
)

DB_NAME = os.getenv("DB_NAME", "postgres")
DB_USER = os.getenv("DB_USER", "exchange_rates_db")
DB_PASSWORD = os.getenv("DB_PASSWORD")

# Local / TCP fallback
DB_HOST = os.getenv("DB_HOST", "34.78.76.12")
DB_PORT = os.getenv("DB_PORT", "5432")

# ---- Connection pool (keep VERY small on Cloud Run) ----

_connection_pool = None


def _create_connection():
    """
    Creates a new database connection.
    Uses Unix socket on Cloud Run, TCP locally.
    """

    # Cloud Run / Cloud SQL (Unix socket)
    if os.path.exists("/cloudsql"):
        return psycopg2.connect(
            host=f"/cloudsql/{INSTANCE_CONNECTION_NAME}",
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
        )

    # Local development / fallback (TCP)
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        sslmode="require",
    )


def get_connection_pool():
    global _connection_pool

    if _connection_pool is None:
        _connection_pool = pool.SimpleConnectionPool(
            minconn=1,
            maxconn=2,  # IMPORTANT: keep this tiny on Cloud Run
            connection_factory=_create_connection,
        )

    return _connection_pool


@contextmanager
def get_db_connection():
    """
    Context manager that safely gets and returns a connection.
    """
    pool_instance = get_connection_pool()
    conn = pool_instance.getconn()

    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        pool_instance.putconn(conn)
