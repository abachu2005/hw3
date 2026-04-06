import csv
import os
import sys
from pymongo import MongoClient

MONGO_URI = os.environ.get("MONGO_URI", "")
BATCH_SIZE = 5000
CSV_FILE = "Electric_Vehicle_Population_Data.csv"

INT_FIELDS = {"Model Year", "Electric Range", "DOL Vehicle ID"}
STR_FIELDS_KEEP = {
    "VIN (1-10)", "County", "City", "State", "Postal Code",
    "Make", "Model", "Electric Vehicle Type",
    "Clean Alternative Fuel Vehicle (CAFV) Eligibility",
    "Legislative District", "Vehicle Location",
    "Electric Utility", "2020 Census Tract",
}


def clean_record(row: dict) -> dict:
    doc = {}
    for key, value in row.items():
        k = key.strip()
        v = value.strip() if value else ""
        if k in INT_FIELDS:
            try:
                v = int(v)
            except (ValueError, TypeError):
                pass
        doc[k] = v
    return doc


def main():
    uri = MONGO_URI
    if not uri:
        if len(sys.argv) > 1:
            uri = sys.argv[1]
        else:
            uri = input("Enter your MongoDB Atlas connection string: ").strip()

    client = MongoClient(uri)
    db = client["ev_db"]
    collection = db["vehicles"]

    collection.drop()
    print("Dropped existing 'vehicles' collection (if any).")

    batch = []
    total = 0

    with open(CSV_FILE, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            batch.append(clean_record(row))
            if len(batch) >= BATCH_SIZE:
                collection.insert_many(batch)
                total += len(batch)
                print(f"Inserted {total} records...")
                batch = []

    if batch:
        collection.insert_many(batch)
        total += len(batch)

    print(f"Done. Total records inserted: {total}")
    client.close()


if __name__ == "__main__":
    main()
