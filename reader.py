import re
from collections import defaultdict
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

                    # Extract timestamp and value
                    data = eval(line.strip())  # Safely evaluate as JSON
                    timestamp = float(data[0][1])
                    value = float(data[0][2])

                    # Insert data into MongoDB
                    collection = db[collection_name]
                    entry = {
                        "timestamp": datetime.utcfromtimestamp(timestamp),
                        "value": value
                    }
                    collection.insert_one(entry)
                    print(f"Stored {currency_pair} data in MongoDB.")

    except FileNotFoundError:
        print(f"Error: Log file '{log_file}' not found.")

    except Exception as e:
        print(f"Error occurred: {e}")

# Example usage:
log_file = 'logs/websocket_responses.log'
parse_and_store_response(log_file)
