import json
import sqlalchemy
import yaml

from sqlalchemy import text
from typing import Dict, Any


class AWSDBConnector:
    """Manages connection to the AWS database."""

    def __init__(self) -> None:
        """
        Initializes the AWSDBConnector with database credentials from a YAML file.
        """
        with open('db_creds.yaml', 'r') as stream:
            creds = yaml.safe_load(stream)
        self.HOST = creds['HOST']
        self.USER = creds['USER']
        self.PASSWORD = creds['PASSWORD']
        self.DATABASE = 'pinterest_data'
        self.PORT = 3306
        
    def create_db_connector(self) -> sqlalchemy.engine.base.Engine:
        """
        Creates and returns a SQLAlchemy engine for connecting to the database.

        Returns:
            sqlalchemy.engine.base.Engine: The SQLAlchemy engine.
        """
        engine = sqlalchemy.create_engine(
            f"mysql+pymysql://{self.USER}:{self.PASSWORD}@{self.HOST}:{self.PORT}/{self.DATABASE}?charset=utf8mb4"
        )
        return engine
    

def get_db_row(table_name: str, random_row: int, connection: sqlalchemy.engine.base.Connection) -> Dict[str, Any]:
    """
    Fetches a single row from the specified database table.

    Args:
        table_name (str): The name of the database table.
        random_row (int): The row number to fetch.
        connection (sqlalchemy.engine.base.Connection): The database connection.

    Returns:
        Dict[str, Any]: The row data as a dictionary.
    """
    query = text(f"SELECT * FROM {table_name} LIMIT {random_row}, 1")
    selected_row = connection.execute(query)
    
    for row in selected_row:
        result = dict(row._mapping)
    return result


def create_kafka_payload(data: Dict) -> str:
    """
    Converts data into a JSON payload suitable for Kafka.

    Args:
        data (Dict): The data to be included in the payload.

    Returns:
        str: The JSON-formatted payload.
    """
    payload = json.dumps({
        "records": [
            {
                "value": data
            }
        ]
        }, indent=4, sort_keys=True, default=str)
    return payload



def create_kinesis_payload(data: Dict, user_id: str, topic_name: str) -> str:
    """
    Creates a JSON payload for sending data to an AWS Kinesis stream.

    Args:
        data (Dict): The data to be sent to the Kinesis stream.
        user_id (str): The user ID used in constructing the stream name.
        topic_name (str): The topic name used in constructing the stream name.

    Returns:
        str: A JSON-formatted string representing the payload to be sent to Kinesis.
    """
    payload = json.dumps({
        "StreamName": f"streaming-{user_id}-{topic_name}",
        "Data": data,
        "PartitionKey": "partition-1"
    }, indent=4, sort_keys=True, default=str)
    
    return payload

