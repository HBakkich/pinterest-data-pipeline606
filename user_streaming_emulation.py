import requests
import random
import db_utils as utils
import endpoints as ep

from time import sleep
from typing import Dict


random.seed(100)

new_connector = utils.AWSDBConnector()


def send_data_to_kinesis(invoke_url: str, data: Dict, user_id: str, topic_name: str) -> None:
    """
    Sends data to the specified Kinesis stream via a PUT request.

    Args:
        invoke_url (str): The URL to send the data to.
        data (Dict): The data to be sent to the Kinesis stream.
        user_id (str): The user ID to create a stream name.
        topic_name (str): The topic name to create a stream name.
    
    Returns:
        None
    """
    payload = utils.create_kinesis_payload(data, user_id, topic_name)

    try:
        response = requests.request("PUT", invoke_url, headers=ep.KINESIS_HEADERS, data=payload)
        if response.status_code != 200:
            print(f"Failed to send data to {invoke_url}: {response.status_code}, {response.text}")
        else:
            print(f"\nData sent successfully:\n {response.text}\n\n")
    except requests.exceptions.RequestException as ex:
        print(f"An error occurred: {ex}")


def run_infinite_post_data_loop() -> None:
    """
    Continuously fetches random rows from the database and sends the data to Kinesis streams.
    
    The loop runs indefinitely, fetching random rows from Pinterest, geolocation, and user data
    tables, and sends them to respective Kinesis streams.

    Returns:
        None
    """
    while True:
        sleep(random.randrange(0, 2))
        random_row = random.randint(0, 11000)
        engine = new_connector.create_db_connector()

        with engine.connect() as connection:
            random_pin_row = utils.get_db_row("pinterest_data", random_row, connection)
            print(random_pin_row)
            send_data_to_kinesis(ep.PIN_INVOKE_URL_KINESIS, random_pin_row, '124f8314c0a1', 'pin')

            random_geo_row = utils.get_db_row("geolocation_data", random_row, connection)
            print(random_geo_row)
            send_data_to_kinesis(ep.GEO_INVOKE_URL_KINESIS, random_geo_row, '124f8314c0a1', 'geo')
            
            random_user_row = utils.get_db_row("pinterest_data", random_row, connection)
            print(random_user_row)
            send_data_to_kinesis(ep.USER_INVOKE_URL_KINESIS, random_user_row, '124f8314c0a1', 'user')


if __name__ == "__main__":
    run_infinite_post_data_loop()
    print('Working')
