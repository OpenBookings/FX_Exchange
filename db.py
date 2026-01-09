import os
import logging
import sqlalchemy
from contextlib import contextmanager


# Set up logging
logger = logging.getLogger(__name__)

# ---- Configuration ----

# Determine connection type: if DB_HOST is set, use direct connection; otherwise use Cloud SQL
DB_HOST = os.getenv("DB_HOST")
USE_CLOUD_SQL = DB_HOST is None

# Direct PostgreSQL connection settings (for databases outside Google Cloud)
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "postgres")
DB_USER = os.getenv("DB_USER", "exchange_rates_db")
DB_PASSWORD = os.getenv("DB_PASSWORD")

# Cloud SQL connection settings (for Google Cloud SQL instances)
INSTANCE_CONNECTION_NAME = os.getenv(
    "INSTANCE_CONNECTION_NAME",
    "openbookings:europe-west1:openbookings-db",
)
IP_TYPE = os.getenv("IP_TYPE", "PUBLIC")

# Security: Require password from environment variable
if not DB_PASSWORD:
    logger.error("DB_PASSWORD environment variable is not set")
    raise ValueError("DB_PASSWORD environment variable is required for database connection")

# ---- Connection setup ----

_connector = None
_engine = None

def _get_connector():
    """Get or create the Cloud SQL Connector instance."""
    global _connector
    if _connector is None:
        try:
            from google.cloud.sql.connector import Connector
            _connector = Connector()
        except ImportError:
            logger.error("cloud-sql-python-connector is not installed. Install it with: pip install cloud-sql-python-connector")
            raise
    return _connector

def _getconn_cloud_sql():
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
        if USE_CLOUD_SQL:
            logger.info(f"Creating SQLAlchemy engine for Cloud SQL instance: {INSTANCE_CONNECTION_NAME}")
            _engine = sqlalchemy.create_engine(
                "postgresql+pg8000://",
                creator=_getconn_cloud_sql,
                pool_size=2,  # Keep pool small on Cloud Run
                max_overflow=0,
                pool_recycle=3600,  # Recycle connections after 1 hour
                pool_pre_ping=True,  # Verify connections before using
            )
        else:
            # Build connection string for direct PostgreSQL connection
            connection_string = f"postgresql+pg8000://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
            logger.info(f"Creating SQLAlchemy engine for direct PostgreSQL connection: {DB_HOST}:{DB_PORT}/{DB_NAME}")
            _engine = sqlalchemy.create_engine(
                connection_string,
                pool_size=5,  # Larger pool for direct connections
                max_overflow=10,
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
            if USE_CLOUD_SQL:
                logger.error(
                    "Cloud SQL connection failed. Please verify:\n"
                    "1. Cloud Run service has Cloud SQL connection configured\n"
                    "2. Cloud SQL instance allows connections from this service\n"
                    "3. Database credentials are correct\n"
                    f"4. IP_TYPE is set correctly (current: {IP_TYPE})\n"
                    f"5. For cross-project access, ensure service account has 'roles/cloudsql.client' role"
                )
            else:
                logger.error(
                    f"PostgreSQL connection failed. Please verify:\n"
                    f"1. DB_HOST is correct and reachable (current: {DB_HOST})\n"
                    f"2. DB_PORT is correct (current: {DB_PORT})\n"
                    f"3. Database server is running and accepting connections\n"
                    f"4. Firewall/network allows connections from this host\n"
                    f"5. Database credentials are correct (DB_USER: {DB_USER}, DB_NAME: {DB_NAME})"
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
