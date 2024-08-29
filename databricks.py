# Databricks notebook source
# pyspark functions
from pyspark.sql.functions import *
# URL processing
import urllib

# Define the path to the Delta table
delta_table_path = "dbfs:/user/hive/warehouse/authentication_credentials"

# Read the Delta table to a Spark DataFrame
aws_keys_df = spark.read.format("delta").load(delta_table_path)

# COMMAND ----------

# Get the AWS access key and secret key from the spark dataframe
ACCESS_KEY = aws_keys_df.select('Access key ID').collect()[0]['Access key ID']
SECRET_KEY = aws_keys_df.select('Secret access key').collect()[0]['Secret access key']
# Encode the secrete key
ENCODED_SECRET_KEY = urllib.parse.quote(string=SECRET_KEY, safe="")

# COMMAND ----------

# AWS S3 bucket name
AWS_S3_BUCKET = "user-124f8314c0a1-bucket"
# Mount name for the bucket
MOUNT_NAME = "/mnt/pinterest-data"
# Source url
SOURCE_URL = "s3n://{0}:{1}@{2}".format(ACCESS_KEY, ENCODED_SECRET_KEY, AWS_S3_BUCKET)

# Unmount the directory if it is already mounted
dbutils.fs.unmount(MOUNT_NAME)

# Mount the drive
dbutils.fs.mount(SOURCE_URL, MOUNT_NAME)

# COMMAND ----------

display(dbutils.fs.ls(MOUNT_NAME))



# COMMAND ----------

# MAGIC %sql
# MAGIC -- Disable format checks during the reading of Delta tables
# MAGIC SET spark.databricks.delta.formatCheck.enabled=false

# COMMAND ----------

# File location and type
# Asterisk(*) indicates reading all the content of the specified file that have .json extension
pin_file_location = "/mnt/pinterest-data/topics/124f8314c0a1.pin/partition=0//*.json"
file_type = "json"
geo_file_location = "/mnt/pinterest-data/topics/124f8314c0a1.geo/partition=0//*.json" 
user_file_location = "/mnt/pinterest-data/topics/124f8314c0a1.user/partition=0//*.json"

infer_schema = "true"

# Read in pin JSON from mounted S3 bucket
pin_df = spark.read.format(file_type) \
.option("inferSchema", infer_schema) \
.load(pin_file_location)
# Read in geo JSON from mounted S3 bucket
geo_df = spark.read.format(file_type) \
.option("inferSchema", infer_schema) \
.load(geo_file_location)
# Read in user JSON from mounted S3 bucket
user_df = spark.read.format(file_type) \
.option("inferSchema", infer_schema) \
.load(user_file_location)

# COMMAND ----------

# Display the Spark dataframes to check their contents
display(pin_df.limit(10))
display(geo_df.limit(10))
display(user_df.limit(10))


# COMMAND ----------



