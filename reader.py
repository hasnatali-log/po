import re
from pymongo import MongoClient
from datetime import datetime

# MongoDB connection string and database name
mongo_uri = "mongodb://localhost:27017/"
database_name = "trading"

# Regular expression pattern to match WebSocket URL and response
pattern = r'^\[\["(\w{6}_?\w*)",'

# MongoDB client and database
client = MongoClient(mongo_uri)
db = client[database_name]

# Function to create time series collection if not exists
def create_time_series_collection(collection_name):
    existing_collections = db.list_collection_names()
    if collection_name not in existing_collections:
        try:
            db.create_collection(
                collection_name,
                timeseries={
                    "timeField": "timestamp",
                    "granularity": "seconds",
                    "bucketMaxSpanSeconds": 60,
                    "bucketRoundingSeconds": 1
                }
            )
            print(f"Created time series collection '{collection_name}'")
        except Exception as e:
            print(f"Error creating time series collection '{collection_name}': {e}")
    else:
        print(f"Collection '{collection_name}' already exists.")

# Function to parse and store WebSocket responses in MongoDB
def parse_and_store_response(log_file):
    try:
        with open(log_file, 'r', encoding='utf-8') as f:
            lines = f.readlines()

            for line in lines:
                match = re.match(pattern, line)
                if match:
                    currency_pair = match.group(1)
                    collection_name = f"{currency_pair}"

                    # Ensure time series collection exists
                    create_time_series_collection(collection_name)

                    # Extract timestamp and value from line data
                    try:
                        data = eval(line.strip())  # Safely evaluate as JSON
                        timestamp = float(data[0][1])
                        value = float(data[0][2])

                        # Prepare document to insert
                        document = {
                            "timestamp": datetime.utcfromtimestamp(timestamp),
                            "value": value
                        }

                        # Insert or update document in time series collection
                        collection = db[collection_name]
                        result = collection.insert_one(document)
                        print(f"Inserted document into '{collection_name}' with id: {result.inserted_id}")

                    except Exception as e:
                        print(f"Error processing line '{line.strip()}': {e}")

    except FileNotFoundError:
        print(f"Error: Log file '{log_file}' not found.")

    except Exception as e:
        print(f"Error occurred: {e}")

# Example usage:
if __name__ == "__main__":
    log_file = 'logs/websocket_responses.log'
    parse_and_store_response(log_file)
