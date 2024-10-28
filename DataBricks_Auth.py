# Databricks notebook source
# Importing urllib for URL processing
import urllib
from pyspark.sql import functions as F

# Define the path to the Delta table where AWS keys are stored
delta_table_path = "dbfs:/user/hive/warehouse/authentication_credentials"

# Reading the Delta table into a Spark DataFrame
aws_keys_df = spark.read.format("delta").load(delta_table_path)

# Get the AWS access key and secret key from the spark dataframe
ACCESS_KEY = aws_keys_df.select('Access key ID').collect()[0]['Access key ID']
SECRET_KEY = aws_keys_df.select('Secret access key').collect()[0]['Secret access key']

# Encoding the secret key to ensure it can be safely used in URLs
ENCODED_SECRET_KEY = urllib.parse.quote(string=SECRET_KEY, safe="")

