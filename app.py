import os
from flask import Flask, request, jsonify
from pymongo import MongoClient, ReadPreference
from pymongo.write_concern import WriteConcern

app = Flask(__name__)

MONGO_URI = os.environ.get("MONGO_URI", "")
client = MongoClient(MONGO_URI)
db = client["ev_db"]
collection = db["vehicles"]


@app.route("/insert-fast", methods=["POST"])
def insert_fast():
    """Primary-only write acknowledgement (w=1). Fast but risks data loss."""
    record = request.get_json()
    coll = collection.with_options(write_concern=WriteConcern(w=1))
    result = coll.insert_one(record)
    return jsonify({"inserted_id": str(result.inserted_id)})


@app.route("/insert-safe", methods=["POST"])
def insert_safe():
    """Majority write acknowledgement (w='majority'). Slower but durable."""
    record = request.get_json()
    coll = collection.with_options(write_concern=WriteConcern(w="majority"))
    result = coll.insert_one(record)
    return jsonify({"inserted_id": str(result.inserted_id)})


@app.route("/count-tesla-primary", methods=["GET"])
def count_tesla_primary():
    """Strong consistency read from PRIMARY only."""
    coll = collection.with_options(read_preference=ReadPreference.PRIMARY)
    count = coll.count_documents({"Make": "TESLA"})
    return jsonify({"count": count})


@app.route("/count-bmw-secondary", methods=["GET"])
def count_bmw_secondary():
    """Eventually consistent read from SECONDARY, falling back to PRIMARY."""
    coll = collection.with_options(
        read_preference=ReadPreference.SECONDARY_PREFERRED
    )
    count = coll.count_documents({"Make": "BMW"})
    return jsonify({"count": count})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
