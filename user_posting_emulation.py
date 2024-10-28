import requests
import random
import db_utils as utils
import endpoints as ep

from time import sleep
from typing import Dict


random.seed(100)

new_connector = utils.AWSDBConnector()


def send_data_to_kafka(invoke_url: str, data: Dict) -> None:
    """
    Sends data to a Kafka topic via HTTP POST request.

    Args:
        invoke_url (str): The URL to send the Kafka data payload to.
        data (Dict): The data payload to send to Kafka.

    Returns:
        None
    """
    payload = utils.create_kafka_payload(data)

    try:
        response = requests.request("POST", invoke_url, headers=ep.KAFKA_HEADERS, data=payload)
        if response.status_code != 200:
            print(f"Failed to send data to {invoke_url}: {response.status_code}, {response.text}")
        else:
            print(f"\nData sent successfully, status code:  {response.status_code}\n\n")

    except requests.exceptions.RequestException as ex:
        print(f"An error occurred: {ex}")


def run_infinite_post_data_loop() -> None:
    """
    Continuously fetches random rows from the database and sends them to Kafka.

    This function runs indefinitely, fetching data from the database and sending it to Kafka at random intervals.

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
            send_data_to_kafka(ep.PIN_INVOKE_URL_KAFKA, random_pin_row)

            random_geo_row = utils.get_db_row("geolocation_data", random_row, connection)
            print(random_geo_row)
            send_data_to_kafka(ep.PIN_INVOKE_URL_KAFKA, random_geo_row)

            random_user_row = utils.get_db_row("pinterest_data", random_row, connection)
            print(random_user_row)
            send_data_to_kafka(ep.PIN_INVOKE_URL_KAFKA, random_user_row)


if __name__ == "__main__":
    run_infinite_post_data_loop()
    print('Working')
