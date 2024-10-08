import requests
import yaml
from time import sleep
import random
from multiprocessing import Process
import boto3
import json
import sqlalchemy
from sqlalchemy import text

random.seed(100)

class AWSDBConnector:

    def __init__(self):
        with open('db_creds.yaml', 'r') as stream:
            creds = yaml.safe_load(stream)
        self.HOST = creds['HOST']
        self.USER = creds['USER']
        self.PASSWORD = creds['PASSWORD']
        self.DATABASE = 'pinterest_data'
        self.PORT = 3306
        
    def create_db_connector(self):
        engine = sqlalchemy.create_engine(f"mysql+pymysql://{self.USER}:{self.PASSWORD}@{self.HOST}:{self.PORT}/{self.DATABASE}?charset=utf8mb4")
        return engine

new_connector = AWSDBConnector()

# Set the API Invoke URLs for each Kafka topic
pin_invoke_url = "https://8nnc5v1d62.execute-api.us-east-1.amazonaws.com/dev/topics/124f8314c0a1.pin"
geo_invoke_url = "https://8nnc5v1d62.execute-api.us-east-1.amazonaws.com/dev/topics/124f8314c0a1.geo"
user_invoke_url = "https://8nnc5v1d62.execute-api.us-east-1.amazonaws.com/dev/topics/124f8314c0a1.user"

def send_data_to_kafka(invoke_url, data):
    payload = json.dumps({
        "records": [
            {
                "value": data
            }
        ]
    }, 
        indent=4, sort_keys=True, default=str)

    headers = {'Content-Type': 'application/vnd.kafka.json.v2+json'}
    response = requests.request("POST", invoke_url, headers=headers, data=payload)
    if response.status_code != 200:
        print(f"Failed to send data to {invoke_url}: {response.status_code}, {response.text}")

def run_infinite_post_data_loop():
    while True:
        sleep(random.randrange(0, 2))
        random_row = random.randint(0, 11000)
        engine = new_connector.create_db_connector()

        with engine.connect() as connection:

            pin_string = text(f"SELECT * FROM pinterest_data LIMIT {random_row}, 1")
            pin_selected_row = connection.execute(pin_string)
            
            for row in pin_selected_row:
                pin_result = dict(row._mapping)
                print(pin_result)
                send_data_to_kafka(pin_invoke_url, pin_result)

            geo_string = text(f"SELECT * FROM geolocation_data LIMIT {random_row}, 1")
            geo_selected_row = connection.execute(geo_string)
            
            for row in geo_selected_row:
                geo_result = dict(row._mapping)
                print(geo_result)
                send_data_to_kafka(geo_invoke_url, geo_result)

            user_string = text(f"SELECT * FROM user_data LIMIT {random_row}, 1")
            user_selected_row = connection.execute(user_string)
            
            for row in user_selected_row:
                user_result = dict(row._mapping)
                print(user_result)
                send_data_to_kafka(user_invoke_url, user_result)

if __name__ == "__main__":
    run_infinite_post_data_loop()
    print('Working')
