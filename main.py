import flask

# CUSTOM MODULES
import db
import update
import conversion

app = flask.Flask(__name__)

@app.route("/update", methods=["GET"])
def update_exchange_rates():
    base_data = update.fetch_ecb_csv()
    prepared_data = update.prepare_data_for_database(base_data)
    print(prepared_data)
    with db.get_db_connection() as conn:
        update.insert_data_into_database(prepared_data, conn)
    return flask.jsonify({"message": "Data inserted into database"}), 200

@app.route("/convert", methods=["GET"])
def convert_amount():
    amount = float(flask.request.args.get("amount"))
    from_ccy = flask.request.args.get("from")
    to_ccy = flask.request.args.get("to")
    
    try:
        with db.get_db_connection() as conn:
            result = conversion.convert_amount(amount, from_ccy, to_ccy, conn)
        return flask.jsonify({
            "amount": result,
            "from": from_ccy,
            "to": to_ccy
        }), 200
    except (KeyError, ValueError) as e:
        return flask.jsonify({"error": str(e)}), 400

if __name__ == "__main__":
    import os
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)