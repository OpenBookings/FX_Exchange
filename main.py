import os
import logging
import flask

# CUSTOM MODULES
import db
import update
import conversion

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

app = flask.Flask(__name__)

@app.route("/health", methods=["GET"])
def health_check():
    """Health check endpoint that verifies database connectivity."""
    logger.info("Health check requested")
    try:
        # Test database connection
        with db.get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            cursor.fetchone()
            cursor.close()
        logger.info("Health check passed - database connection successful")
        return flask.jsonify({
            "status": "healthy",
            "database": "connected",
            "db_name": db.DB_NAME
        }), 200
    except Exception as e:
        logger.error(f"Health check failed: {e}", exc_info=True)
        return flask.jsonify({
            "status": "unhealthy",
            "database": "disconnected",
            "db_name": db.DB_NAME,
            "error": str(e)
        }), 503

#test db connection 
@app.route("/test-db", methods=["GET"]) 
def test_db():
    """Test database connection and return sample data."""
    logger.info("Database test requested")
    try:
        with db.get_db_connection() as conn:
            result = update.test_db_connection(conn)
            logger.info(f"Database test successful, returned {len(result) if result else 0} rows")
            return flask.jsonify({"status": "connected to database", "result": result}), 200
    except Exception as e:
        logger.error(f"Database test failed: {e}", exc_info=True)
        return flask.jsonify({"status": "error", "error": str(e)}), 500


@app.route("/update", methods=["GET"])
def update_exchange_rates():
    """Fetch and update exchange rates from ECB API."""
    logger.info("Exchange rate update requested")
    try:
        logger.info("Fetching exchange rates from ECB API")
        base_data = update.fetch_ecb_csv()
        
        logger.info("Preparing data for database insertion")
        prepared_data = update.prepare_data_for_database(base_data)
        logger.info(f"Prepared {len(prepared_data)} exchange rate records for insertion")
        
        if not prepared_data:
            logger.warning("No new exchange rate data to insert")
            return flask.jsonify({"message": "No new data to insert"}), 200
        
        logger.info("Inserting data into database")
        with db.get_db_connection() as conn:
            update.insert_data_into_database(prepared_data, conn)
        
        logger.info(f"Successfully inserted {len(prepared_data)} exchange rate records")
        return flask.jsonify({
            "message": "Data inserted into database",
            "records_inserted": len(prepared_data)
        }), 200
    except Exception as e:
        logger.error(f"Failed to update exchange rates: {e}", exc_info=True)
        return flask.jsonify({"error": str(e)}), 500

@app.route("/convert", methods=["GET"])
def convert_amount():
    """Convert amount from one currency to another."""
    # Get and validate input parameters
    amount_str = flask.request.args.get("amount")
    from_ccy = flask.request.args.get("from")
    to_ccy = flask.request.args.get("to")
    
    # Validate required parameters
    if not all([amount_str, from_ccy, to_ccy]):
        logger.warning(f"Missing required parameters - amount: {amount_str}, from: {from_ccy}, to: {to_ccy}")
        return flask.jsonify({
            "error": "Missing required parameters: amount, from, to"
        }), 400
    
    # Validate and convert amount
    try:
        amount = float(amount_str)
        if amount < 0:
            logger.warning(f"Invalid amount: {amount} (must be >= 0)")
            return flask.jsonify({"error": "amount must be >= 0"}), 400
    except (ValueError, TypeError):
        logger.warning(f"Invalid amount parameter: {amount_str}")
        return flask.jsonify({"error": "Invalid amount parameter"}), 400
    
    logger.info(f"Currency conversion requested: {amount} {from_ccy} -> {to_ccy}")
    
    try:
        with db.get_db_connection() as conn:
            result = conversion.convert_amount(amount, from_ccy, to_ccy, conn)
        logger.info(f"Conversion successful: {amount} {from_ccy} = {result} {to_ccy}")
        return flask.jsonify({
            "amount": result,
            "from": from_ccy.upper(),
            "to": to_ccy.upper(),
            "original_amount": amount
        }), 200
    except KeyError as e:
        logger.warning(f"Exchange rate not found: {e}")
        return flask.jsonify({"error": str(e)}), 404
    except ValueError as e:
        logger.warning(f"Invalid conversion request: {e}")
        return flask.jsonify({"error": str(e)}), 400
    except Exception as e:
        logger.error(f"Conversion failed: {e}", exc_info=True)
        return flask.jsonify({"error": "Internal server error"}), 500

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    debug_mode = os.environ.get("FLASK_DEBUG", "false").lower() == "true"
    logger.info(f"Starting Flask application on port {port} (debug={debug_mode})")
    app.run(host="0.0.0.0", port=port, debug=debug_mode)