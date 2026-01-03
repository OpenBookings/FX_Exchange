import flask

# CUSTOM MODULES
import db
import update
import convertion

app = flask.Flask(__name__)

@app.route("/update", methods=["GET"])
def update_exchange_rates():
    method = flask.request.args.get("method")
    base_data = update.fetch_ecb_csv()
    prepared_data = update.prepare_data_for_database(base_data)
    print(prepared_data)
    with db.get_db_connection(method) as conn:
        if input("Are you sure you want to insert data into database? (y/n): ") == "y":
            update.insert_data_into_database(prepared_data, conn)
            return flask.jsonify({"message": "Data inserted into database"}), 200
        else:
            return flask.jsonify({"message": "Data not inserted into database"}), 200

@app.route("/convert", methods=["GET"])
def convert_amount():
    amount = float(flask.request.args.get("amount"))
    from_ccy = flask.request.args.get("from")
    to_ccy = flask.request.args.get("to")
    method = 'production'
    
    try:
        with db.get_db_connection(method) as conn:
            result = convertion.convert_amount(amount, from_ccy, to_ccy, conn)
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