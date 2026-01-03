import os
from typing import Optional
from contextlib import contextmanager
from psycopg2 import pool
import psycopg2

# PostgreSQL connection configuration
# Either DB_CONNECTION_STRING or individual DB_* variables must be set
# For production, prefer DB_CONNECTION_STRING for security and flexibility
DB_CONNECTION_STRING = os.getenv("DB_CONNECTION_STRING")
DB_HOST = os.getenv("DB_HOST", "34.78.76.12")
DB_PORT = os.getenv("DB_PORT", "5432")  # Default port is standard
DB_NAME = os.getenv("DB_NAME", "postgres")
DB_USER = os.getenv("DB_USER", "exchange_rates_db")
DB_PASSWORD = os.getenv("DB_PASSWORD", f"FFF@,OvOqBru6A)j")

# Connection pool
_connection_pool: Optional[pool.SimpleConnectionPool] = None


def get_connection_pool(method: str):
    """Initialize and return a connection pool."""
    global _connection_pool

    if method == "production":
        print("Production connection pool")
        if _connection_pool is None:
            if not DB_CONNECTION_STRING:
                raise ValueError(
                    "Database configuration missing. Set DB_CONNECTION_STRING environment variable"
                )
            _connection_pool = pool.SimpleConnectionPool(1, 10, DB_CONNECTION_STRING)
        return _connection_pool
    elif method == "development":
        print("Development connection pool")
        if _connection_pool is None:
            if not (DB_HOST and DB_PORT and DB_NAME and DB_USER and DB_PASSWORD):
                raise ValueError(
                    "Database configuration missing. Set DB_HOST, DB_PORT, DB_NAME, DB_USER, and DB_PASSWORD (or *_DEVELOPMENT variants) environment variables"
                )
            _connection_pool = pool.SimpleConnectionPool(
                1, 10,
                host=DB_HOST,
                port=DB_PORT,
                database=DB_NAME,
                user=DB_USER,
                password=DB_PASSWORD
            )
        return _connection_pool
    else:
        raise ValueError(f"Invalid method: {method}")

@contextmanager
def get_db_connection(method: str):
    """Get a database connection from the pool."""
    pool_instance = get_connection_pool(method)
    conn = pool_instance.getconn()
    try:
        yield conn
    finally:
        pool_instance.putconn(conn)