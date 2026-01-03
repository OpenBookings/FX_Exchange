import os
import logging
from psycopg2 import pool, OperationalError
from contextlib import contextmanager

# Set up logging
logger = logging.getLogger(__name__)

# ---- Configuration ----

INSTANCE_CONNECTION_NAME = os.getenv(
    "INSTANCE_CONNECTION_NAME",
    "openbookings:europe-west1:openbookings-db",
)

DB_NAME = os.getenv("DB_NAME", "postgres")
DB_USER = os.getenv("DB_USER", "exchange_rates_db")
DB_PASSWORD = os.getenv("DB_PASSWORD", "Byz^NsKnMjBb6/tO")

# Local / TCP fallback
DB_HOST = os.getenv("DB_HOST", "34.78.76.12")
DB_PORT = os.getenv("DB_PORT", "5432")

# ---- Connection pool (keep VERY small on Cloud Run) ----

_connection_pool = None

def _get_socket_path():
    """Get the Unix socket path for Cloud SQL connection."""
    return f"/cloudsql/{INSTANCE_CONNECTION_NAME}/.s.PGSQL.5432"

def _use_unix_socket():
    """Check if Unix socket connection should be used."""
    socket_path = _get_socket_path()
    # Check if the socket file actually exists, not just the directory
    return os.path.exists(socket_path)

def _create_connection_pool(dsn, use_socket=False):
    """Create a connection pool with the given DSN."""
    try:
        return pool.SimpleConnectionPool(
            minconn=1,
            maxconn=2,  # IMPORTANT: keep this tiny on Cloud Run
            dsn=dsn,
        )
    except OperationalError as e:
        error_msg = str(e)
        if "password authentication failed" in error_msg.lower():
            logger.error(
                f"Database authentication failed. Please verify:\n"
                f"1. DB_USER is correct (current: {DB_USER})\n"
                f"2. DB_PASSWORD is correct\n"
                f"3. User has proper permissions on database '{DB_NAME}'"
            )
        if use_socket:
            logger.warning(f"Failed to create connection pool with Unix socket: {e}")
            logger.info("Falling back to TCP connection")
            return None
        raise

def get_connection_pool():
    global _connection_pool

    if _connection_pool is None:
        # Try Unix socket first (Cloud Run / Cloud SQL)
        if _use_unix_socket():
            socket_path = f"/cloudsql/{INSTANCE_CONNECTION_NAME}"
            dsn = (
                f"host={socket_path} "
                f"dbname={DB_NAME} "
                f"user={DB_USER} "
                f"password={DB_PASSWORD}"
            )
            logger.info(f"Attempting Unix socket connection: {socket_path}")
            _connection_pool = _create_connection_pool(dsn, use_socket=True)
            
            # If socket connection failed, fall back to TCP
            if _connection_pool is None:
                dsn = (
                    f"host={DB_HOST} "
                    f"port={DB_PORT} "
                    f"dbname={DB_NAME} "
                    f"user={DB_USER} "
                    f"password={DB_PASSWORD} "
                    f"sslmode=require"
                )
                logger.info(f"Using TCP connection: {DB_HOST}:{DB_PORT}")
                _connection_pool = _create_connection_pool(dsn, use_socket=False)
        else:
            # Local development / fallback (TCP)
            dsn = (
                f"host={DB_HOST} "
                f"port={DB_PORT} "
                f"dbname={DB_NAME} "
                f"user={DB_USER} "
                f"password={DB_PASSWORD} "
                f"sslmode=require"
            )
            logger.info(f"Using TCP connection (socket not available): {DB_HOST}:{DB_PORT}")
            _connection_pool = _create_connection_pool(dsn, use_socket=False)

    return _connection_pool

@contextmanager
def get_db_connection():
    """
    Context manager that safely gets and returns a connection.
    Handles connection failures and provides better error messages.
    """
    pool_instance = get_connection_pool()
    conn = None
    
    try:
        conn = pool_instance.getconn()
    except OperationalError as e:
        logger.error(f"Failed to get connection from pool: {e}")
        # If this is a connection refused error, it might mean the socket isn't working
        # even though it exists. This could indicate Cloud SQL isn't properly configured.
        if "Connection refused" in str(e) or "connection to server" in str(e).lower():
            logger.error(
                "Cloud SQL connection refused. Please verify:\n"
                "1. Cloud Run service has Cloud SQL connection configured\n"
                "2. Cloud SQL instance allows connections from this service\n"
                "3. Database credentials are correct"
            )
        raise

    try:
        yield conn
        conn.commit()
    except Exception:
        if conn:
            conn.rollback()
        raise
    finally:
        if conn:
            pool_instance.putconn(conn)
