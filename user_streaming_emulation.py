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

# Set the API Invoke URLs for each Kinesis topic
pin_invoke_url = "https://8nnc5v1d62.execute-api.us-east-1.amazonaws.com/dev/streams/streaming-124f8314c0a1-pin/record"
geo_invoke_url = "https://8nnc5v1d62.execute-api.us-east-1.amazonaws.com/dev/streams/streaming-124f8314c0a1-geo/record"
user_invoke_url = "https://8nnc5v1d62.execute-api.us-east-1.amazonaws.com/dev/streams/streaming-124f8314c0a1-user/record"

def send_data_to_kinesis(invoke_url, data, user_id, topic_name):
    payload = json.dumps({
            "StreamName": f"streaming-{user_id}-{topic_name}",
            "Data": data,
                    "PartitionKey": "partition-1"
             })

    headers = {'Content-Type': 'application/json'}
    response = requests.request("PUT", invoke_url, headers=headers, data=payload)
    print(response.status_code)
    print(response.json())
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
                send_data_to_kinesis(pin_invoke_url, pin_result, '124f8314c0a1', 'pin')

            geo_string = text(f"SELECT * FROM geolocation_data LIMIT {random_row}, 1")
            geo_selected_row = connection.execute(geo_string)
            
            for row in geo_selected_row:
                geo_result = dict(row._mapping)
                geo_result['timestamp'] = geo_result['timestamp'].isoformat()
                print(geo_result)
                send_data_to_kinesis(geo_invoke_url, geo_result, '124f8314c0a1', 'geo')

            user_string = text(f"SELECT * FROM user_data LIMIT {random_row}, 1")
            user_selected_row = connection.execute(user_string)
            
            for row in user_selected_row:
                user_result = dict(row._mapping)
                user_result['date_joined'] = user_result['date_joined'].isoformat()
                print(user_result)
                send_data_to_kinesis(user_invoke_url, user_result, '124f8314c0a1', 'user')


if __name__ == "__main__":
    run_infinite_post_data_loop()
    print('Working')