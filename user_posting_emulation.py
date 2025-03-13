# Required Libraries
import requests
from time import sleep
import random
from multiprocessing import Process
import boto3
import json
import yaml
import sqlalchemy
from sqlalchemy import text
import datetime
import time

# -------------------- DATABASE CONNECTOR --------------------
class AWSDBConnector:
    def __init__(self, DB_CREDS_PATH="db_creds.yaml"):
        """Initialize database credentials from a YAML file."""
        with open(DB_CREDS_PATH, 'r') as file:
            credentials = yaml.safe_load(file)

        db_creds = credentials["database_login"]

        # Assign database connection details
        self.HOST = db_creds["host"]
        self.USER = db_creds["user"]
        self.PASSWORD = db_creds["password"]
        self.DATABASE = db_creds["database"]
        self.PORT = db_creds["port"]

    def create_db_connector(self):
        """Create a SQLAlchemy database connector."""
        engine = sqlalchemy.create_engine(
            f"mysql+pymysql://{self.USER}:{self.PASSWORD}@{self.HOST}:{self.PORT}/{self.DATABASE}?charset=utf8mb4"
        )
        return engine


# Initialize Database Connector
new_connector = AWSDBConnector()


# -------------------- KAFKA CONFIGURATION --------------------
BASE_INVOKE_URL = "https://4aucc3tht0.execute-api.us-east-1.amazonaws.com/dev"
HEADERS = {'Content-Type': 'application/vnd.kafka.json.v2+json'}


# -------------------- KAFKA PRODUCER --------------------
def send_to_kafka(topic_name, record, max_retries=5, timeout=10):
    """Send JSON payload to Kafka topic using REST Proxy with retries and exponential backoff."""
    payload = {"records": [{"value": record}]}  # Direct JSON payload
    invoke_url = f"{BASE_INVOKE_URL}/topics/{topic_name}"

    #Serialize payload using json.dumps() with default=str
    serialized_payload = json.dumps(payload, default=str)

    for attempt in range(max_retries):
        try:
            #Use data= and not json= since we're passing pre-serialized data
            response = requests.post(invoke_url, headers=HEADERS, data=serialized_payload, timeout=timeout)

            if response.status_code == 200:
                print(f"✅ Record sent to {topic_name} (Status Code: {response.status_code})")
                return response.status_code
            else:
                print(f"❌ Failed to send record to {topic_name}. Status: {response.status_code}, Response: {response.text}")

        except requests.exceptions.RequestException as e:
            print(f"⚠️ Network error (attempt {attempt+1}/{max_retries}): {e}")

        # Exponential backoff with jitter
        sleep_time = 2 ** attempt + random.uniform(0, 1)
        print(f"⏳ Retrying in {sleep_time:.2f} seconds...")
        time.sleep(sleep_time)

    print(f"❌ Max retries reached for {topic_name}. Skipping record.")
    return None


# -------------------- FETCH RECORDS IN ORDER --------------------
def fetch_sequential_row(connection, table_name, row_index):
    """Fetch a single row from a specified table in sequential order."""
    query = text(f"SELECT * FROM {table_name} LIMIT {row_index}, 1")
    result = connection.execute(query)
    for row in result:
        return dict(row._mapping)
    return None


def get_total_rows(connection, table_name):
    """Get the total number of rows in a table."""
    result = connection.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
    return result.scalar()  # Returns the count as an integer


# -------------------- MAIN LOOP FUNCTION --------------------
def run_post_data_loop():
    """Send all records from each table in order."""
    engine = new_connector.create_db_connector()

    with engine.connect() as connection:
        total_rows = get_total_rows(connection, "pinterest_data")  # Assuming all tables have equal rows

        for row_index in range(total_rows):
            # Sequentially fetch records from each table
            pin_result = fetch_sequential_row(connection, "pinterest_data", row_index)
            geo_result = fetch_sequential_row(connection, "geolocation_data", row_index)
            user_result = fetch_sequential_row(connection, "user_data", row_index)

            # Send records to corresponding Kafka topics
            if pin_result:
                send_to_kafka("b194464884bf.pin", pin_result)
            if geo_result:
                send_to_kafka("b194464884bf.geo", geo_result)
            if user_result:
                send_to_kafka("b194464884bf.user", user_result)

            # Fixed delay to prevent rate limiting
            sleep(1)


# -------------------- SCRIPT EXECUTION --------------------
if __name__ == "__main__":
    run_post_data_loop()
    print('✅ Process Complete: All records sent to each Kafka topic in sequential order!')


