import os
import logging
from google.cloud.sql.connector import Connector
import sqlalchemy
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
DB_PASSWORD = os.getenv("DB_PASSWORD")

# Security: Require password from environment variable or Secret Manager
if not DB_PASSWORD:
    logger.error("DB_PASSWORD environment variable is not set")
    raise ValueError("DB_PASSWORD environment variable is required for database connection")

# IP type for Cloud SQL connection (PUBLIC or PRIVATE)
IP_TYPE = os.getenv("IP_TYPE", "PUBLIC")

# ---- Connection setup ----

_connector = None
_engine = None

def _get_connector():
    """Get or create the Cloud SQL Connector instance."""
    global _connector
    if _connector is None:
        _connector = Connector()
    return _connector

def _getconn():
    """Create a connection using the Cloud SQL Connector."""
    logger.debug(f"Attempting to connect to Cloud SQL instance: {INSTANCE_CONNECTION_NAME}")
    connector = _get_connector()
    try:
        conn = connector.connect(
            INSTANCE_CONNECTION_NAME,
            "pg8000",
            user=DB_USER,
            password=DB_PASSWORD,
            db=DB_NAME,
            ip_type=IP_TYPE
        )
        logger.debug("Successfully established connection to Cloud SQL")
        return conn
    except Exception as e:
        logger.error(f"Failed to connect to Cloud SQL: {e}", exc_info=True)
        raise

def get_engine():
    """Get or create the SQLAlchemy engine."""
    global _engine
    if _engine is None:
        logger.info(f"Creating SQLAlchemy engine for Cloud SQL instance: {INSTANCE_CONNECTION_NAME}")
        _engine = sqlalchemy.create_engine(
            "postgresql+pg8000://",
            creator=_getconn,
            pool_size=2,  # Keep pool small on Cloud Run
            max_overflow=0,
            pool_recycle=3600,  # Recycle connections after 1 hour
            pool_pre_ping=True,  # Verify connections before using
        )
    return _engine

@contextmanager
def get_db_connection():
    """
    Context manager that safely gets and returns a database connection.
    Handles connection failures and provides better error messages.
    """
    logger.debug("Getting database connection from pool")
    engine = get_engine()
    conn = None
    
    try:
        # Get a connection from the engine and extract the underlying database connection
        sqlalchemy_conn = engine.connect()
        # Get the underlying database connection
        conn = sqlalchemy_conn.connection
        # We need to keep a reference to the SQLAlchemy connection to close it properly
        conn._sqlalchemy_conn = sqlalchemy_conn
        logger.debug("Database connection acquired successfully")
    except Exception as e:
        logger.error(f"Failed to get connection from engine: {e}", exc_info=True)
        error_msg = str(e).lower()
        if "password authentication failed" in error_msg:
            logger.error(
                f"Database authentication failed. Please verify:\n"
                f"1. DB_USER is correct (current: {DB_USER})\n"
                f"2. DB_PASSWORD is correct\n"
                f"3. User has proper permissions on database '{DB_NAME}'"
            )
        elif "connection" in error_msg or "connect" in error_msg:
            logger.error(
                "Cloud SQL connection failed. Please verify:\n"
                "1. Cloud Run service has Cloud SQL connection configured\n"
                "2. Cloud SQL instance allows connections from this service\n"
                "3. Database credentials are correct\n"
                f"4. IP_TYPE is set correctly (current: {IP_TYPE})\n"
                f"5. For cross-project access, ensure service account has 'roles/cloudsql.client' role"
            )
        raise

    try:
        yield conn
        conn.commit()
        logger.debug("Database transaction committed successfully")
    except Exception as e:
        logger.warning(f"Database transaction failed, rolling back: {e}")
        if conn:
            conn.rollback()
        raise
    finally:
        if conn and hasattr(conn, '_sqlalchemy_conn'):
            # Close the SQLAlchemy connection wrapper
            conn._sqlalchemy_conn.close()
            logger.debug("Database connection closed")
