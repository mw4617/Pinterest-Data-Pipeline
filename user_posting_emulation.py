import requests 
from time import sleep  
import random  
from multiprocessing import Process  
import boto3  
import json  
import yaml  
import sqlalchemy  
from sqlalchemy import text, exc  
import datetime 
import time  

# -------------------- DATABASE CONNECTOR --------------------
class AWSDBConnector:
    """
    Handles connection to an AWS-hosted MySQL database using SQLAlchemy.

    Args:
        db_creds_path (str): Path to the YAML file containing database credentials.

    Raises:
        RuntimeError: If the credentials file is missing.
        ValueError: If required database credentials are incomplete.
    """

    def __init__(self, db_creds_path="db_creds.yaml"):
        """Initialize database credentials from a YAML file."""
        try:
            with open(db_creds_path, 'r') as file:
                credentials = yaml.safe_load(file)  # Load credentials from YAML file
        except FileNotFoundError:
            raise FileNotFoundError("Database credentials file not found!") from e

        # Extract credentials
        db_creds = credentials.get("database_login", {})
        self.HOST = db_creds.get("host")
        self.USER = db_creds.get("user")
        self.PASSWORD = db_creds.get("password")
        self.DATABASE = db_creds.get("database")
        self.PORT = db_creds.get("port")

        # Validate credentials
        if not all([self.HOST, self.USER, self.PASSWORD, self.DATABASE, self.PORT]):
            raise ValueError("Incomplete database credentials!")

    def create_db_connector(self):
        """
        Creates and returns a SQLAlchemy database connector with connection pooling.

        Returns:
            sqlalchemy.engine.Engine: A SQLAlchemy engine for connecting to MySQL.

        Raises:
            RuntimeError: If the database connection fails.
        """
        try:
            engine = sqlalchemy.create_engine(
                f"mysql+pymysql://{self.USER}:{self.PASSWORD}@{self.HOST}:{self.PORT}/{self.DATABASE}?charset=utf8mb4",
                pool_size=5, max_overflow=10, pool_recycle=1800  # Connection pooling settings
            )
            return engine
        except exc.SQLAlchemyError as e:
            raise RuntimeError(f"Database connection error: {e}")

# Initialize Database Connector
new_connector = AWSDBConnector()

# -------------------- KAFKA CONFIGURATION --------------------
BASE_INVOKE_URL = "https://4aucc3tht0.execute-api.us-east-1.amazonaws.com/dev"  # Kafka REST Proxy URL
HEADERS = {'Content-Type': 'application/vnd.kafka.json.v2+json'}  # Kafka request headers

# -------------------- KAFKA PRODUCER --------------------
def send_to_kafka(topic_name, record, max_retries=5, timeout=10):
    """
    Sends a JSON payload to a specified Kafka topic using REST Proxy with retry logic.

    Args:
        topic_name (str): The Kafka topic name.
        record (dict): The JSON payload to be sent.
        max_retries (int): Maximum number of retry attempts. Default is 5.
        timeout (int): Maximum time (in seconds) to wait before retrying.

    Returns:
        int or None: HTTP status code (if successful) or None (if all retries fail).

    Raises:
        requests.exceptions.RequestException: If a network error occurs.
    """
    payload = {"records": [{"value": record}]}  # Format payload as per Kafka REST Proxy
    invoke_url = f"{BASE_INVOKE_URL}/topics/{topic_name}"
    serialized_payload = json.dumps(payload, default=str)  # Convert to JSON format

    for attempt in range(max_retries):
        try:
            response = requests.post(invoke_url, headers=HEADERS, data=serialized_payload, timeout=timeout)
            
            if response.status_code == 200:
                print(f"Record sent to {topic_name} (Status Code: {response.status_code})")
                return response.status_code
            else:
                print(f"Failed to send record to {topic_name}. Status: {response.status_code}, Response: {response.text}")

        except requests.exceptions.RequestException as e:
            print(f"Network error (attempt {attempt+1}/{max_retries}): {e}")

        # Exponential backoff with jitter
        sleep_time = min(2 ** attempt + random.uniform(0, 1), timeout)
        print(f"Retrying in {sleep_time:.2f} seconds...")
        sleep(sleep_time)

    print(f"Max retries reached for {topic_name}. Skipping record.")
    return None

# -------------------- FETCH RANDOM RECORDS --------------------
def fetch_random_row(connection, table_name, total_rows):
    """
    Fetches a single random row from a specified database table.

    Args:
        connection (sqlalchemy.engine.Connection): Active database connection.
        table_name (str): Name of the table from which to fetch data.
        total_rows (int): Total number of rows in the table.

    Returns:
        dict or None: A dictionary representing the fetched row, or None if no data is found.

    Raises:
        sqlalchemy.exc.SQLAlchemyError: If a database error occurs.
    """
    if total_rows == 0:
        return None
    random_index = random.randint(0, total_rows - 1)  # Select random row index
    try:
        query = text("SELECT * FROM {} LIMIT :random_index, 1".format(table_name))
        result = connection.execute(query, {"random_index": random_index})
        row = result.fetchone()
        return dict(row._mapping) if row else None
    except exc.SQLAlchemyError as e:
        print(f"Database error fetching random row from {table_name}: {e}")
        return None


def get_total_rows(connection, table_name):
    """
    Retrieves the total number of rows in a given table.

    Args:
        connection (sqlalchemy.engine.Connection): Active database connection.
        table_name (str): Name of the table.

    Returns:
        int: Total number of rows in the table (0 if an error occurs).

    Raises:
        sqlalchemy.exc.SQLAlchemyError: If a database error occurs.
    """
    try:
        result = connection.execute(text("SELECT COUNT(*) FROM {}".format(table_name)))
        return result.scalar() or 0  # Return row count or 0 if no records exist
    except exc.SQLAlchemyError as e:
        print(f"Error fetching total row count for {table_name}: {e}")
        return 0

# -------------------- MAIN LOOP FUNCTION --------------------
def run_post_data_loop():
    """
    Continuously fetches and sends random records from each database table 
    to their respective Kafka topics until 2000 records are sent.

    Raises:
        Exception: If a critical error occurs during execution.
    """
    engine = new_connector.create_db_connector()
    sent_counts = {"pin": 0, "geo": 0, "user": 0}  # Track sent records per topic

    with engine.begin() as connection:
        total_rows = get_total_rows(connection, "pinterest_data")

        while min(sent_counts.values()) < 2000:  # Run until 2000 records sent per topic
            for topic, table in {"pin": "pinterest_data", "geo": "geolocation_data", "user": "user_data"}.items():
                if sent_counts[topic] < 2000:
                    result = fetch_random_row(connection, table, total_rows)
                    if result:
                        send_to_kafka(f"b194464884bf.{topic}", result)
                        sent_counts[topic] += 1
                        print(f"Total {topic} records sent: {sent_counts[topic]}")
            sleep(random.uniform(0.5, 1.5))  # Add random delay between loops

# -------------------- SCRIPT EXECUTION --------------------
if __name__ == "__main__":
    """
    Entry point of the script. Runs the Kafka producer loop.
    """
    try:
        run_post_data_loop()
        print('Process Complete: 2000 random records sent to each Kafka topic!')
    except Exception as e:
        print(f"Critical error occurred: {e}")
