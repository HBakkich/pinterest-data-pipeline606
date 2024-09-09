# Databricks notebook source
# Importing necessary functions from pyspark.sql
from pyspark.sql.functions import *
# Importing urllib for URL processing
import urllib

# Define the path to the Delta table where AWS keys are stored
delta_table_path = "dbfs:/user/hive/warehouse/authentication_credentials"

# Reading the Delta table into a Spark DataFrame
aws_keys_df = spark.read.format("delta").load(delta_table_path)

# COMMAND ----------

# Get the AWS access key and secret key from the spark dataframe
ACCESS_KEY = aws_keys_df.select('Access key ID').collect()[0]['Access key ID']
SECRET_KEY = aws_keys_df.select('Secret access key').collect()[0]['Secret access key']

# Encoding the secret key to ensure it can be safely used in URLs
ENCODED_SECRET_KEY = urllib.parse.quote(string=SECRET_KEY, safe="")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Disable format checks during the reading of Delta tables
# MAGIC SET spark.databricks.delta.formatCheck.enabled=false

# COMMAND ----------

# MAGIC %md
# MAGIC #### Milestone 9
# MAGIC #### Task 4
# MAGIC ##### Step 3

# COMMAND ----------

# Setting up a streaming DataFrame to read data from AWS Kinesis
pin_stream_df = spark \
.readStream \
.format('kinesis') \
.option('streamName','streaming-124f8314c0a1-pin') \  # Name of the Kinesis stream
.option('initialPosition','earliest') \  # Start reading from the earliest data record
.option('region','us-east-1') \  # AWS region where the Kinesis stream is located
.option('awsAccessKey', ACCESS_KEY) \  # AWS access key for authentication
.option('awsSecretKey', SECRET_KEY) \  # AWS secret key for authentication
.load()

# Uncomment the below line to display the streaming data in the notebook
# display(pin_stream_df)

# COMMAND ----------

# Setting up a streaming DataFrame to read data from AWS Kinesis for geographical data
geo_stream_df = spark \
.readStream \
.format('kinesis') \
.option('streamName','streaming-124f8314c0a1-geo') \  # Name of the Kinesis stream for geographical data
.option('initialPosition','earliest') \  # Start reading from the earliest data record
.option('region','us-east-1') \  # AWS region where the Kinesis stream is located
.option('awsAccessKey', ACCESS_KEY) \  # AWS access key for authentication
.option('awsSecretKey', SECRET_KEY) \  # AWS secret key for authentication
.load()

# Uncomment to view streaming data
# display(geo_stream_df)

# COMMAND ----------

# Setting up a streaming DataFrame to read data from AWS Kinesis for user data
user_stream_df = spark \
.readStream \
.format('kinesis') \
.option('streamName','streaming-124f8314c0a1-user') \  # Name of the Kinesis stream for user data
.option('initialPosition','earliest') \  # Start reading from the earliest data record
.option('region','us-east-1') \  # AWS region where the Kinesis stream is located
.option('awsAccessKey', ACCESS_KEY) \  # AWS access key for authentication
.option('awsSecretKey', SECRET_KEY) \  # AWS secret key for authentication
.load()

# Uncomment to view streaming data
# display(user_stream_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Task 5

# COMMAND ----------

# Define the schema for the data structure of the incoming pin stream
pin_schema = StructType([
    StructField("index", StringType()),  # Unique index for each record
    StructField("unique_id", StringType()),  # Unique identifier for the pin
    StructField("title", StringType()),  # Title of the pin
    StructField("description", StringType()),  # Description of the pin
    StructField("follower_count", StringType()),  # Follower count, stored as string to accommodate suffixes like 'k' or 'M'
    StructField("poster_name", StringType()),  # Name of the user who posted the pin
    StructField("tag_list", StringType()),  # List of tags associated with the pin
    StructField("is_image_or_video", StringType()),  # Indicator if the pin is an image or video
    StructField("image_src", StringType()),  # Source URL of the pin's image
    StructField("downloaded", StringType()),  # Indicates if the pin was downloaded, assumed to be StringType
    StructField("save_location", StringType()),  # Location where the pin was saved
    StructField("category", StringType())  # Category of the pin
])

# Apply the defined schema to the streaming DataFrame by casting the data as STRING
# and then using from_json to apply the schema, extracting the structured data
pin_df = pin_stream_df \
    .selectExpr("CAST(data as STRING)") \
    .select(from_json(col("data"), pin_schema).alias("pin_data")) \
    .select("pin_data.*")  # Flatten the structure to directly access fields

# COMMAND ----------

# Clean and transform the pin_df DataFrame

# Update the "category" column: if null, set to None, otherwise keep the original value
pin_df_cleaned = pin_df.withColumn("category", when(pin_df["category"].isNull(), None).otherwise(pin_df["category"]))

# Update the "description" column in the same way as "category"
pin_df_cleaned = pin_df.withColumn("description", when(pin_df["description"].isNull(), None).otherwise(pin_df["description"]))

# Convert "follower_count" from string to integer, handling 'k' (thousands) and 'M' (millions) suffixes
# For example, '1k' becomes 1000, '1M' becomes 1000000
pin_df_cleaned = pin_df_cleaned.withColumn(
    "follower_count",
    when(col("follower_count").endswith("k"), 
         regexp_replace(col("follower_count"), "k", "").cast("int") * 1000)
    .when(col("follower_count").endswith("M"), 
          regexp_replace(col("follower_count"), "M", "").cast("int") * 1000000)
    .otherwise(col("follower_count").cast("int"))
)

# Ensure "follower_count" is an integer; if not possible, set to None
pin_df_cleaned = pin_df_cleaned.withColumn(
    "follower_count",
    when(
        pin_df_cleaned["follower_count"].cast("int").isNotNull(),
        pin_df_cleaned["follower_count"].cast("int")
    ).otherwise(None)
)

# Convert "downloaded" column to integer type
pin_df_cleaned = pin_df_cleaned.withColumn("downloaded", col("downloaded").cast("int"))

# Remove the prefix "Local save in " from the "save_location" column
pin_df_cleaned = pin_df_cleaned.withColumn("save_location", regexp_replace(col("save_location"), "^Local save in ", ""))

# Rename the "index" column to "ind" for consistency or clarity
pin_df_cleaned = pin_df_cleaned.withColumnRenamed("index", "ind")

# Reorder the columns to a desired sequence for better readability or to match a target schema
desired_column_order = ["ind", "unique_id", "title", "description", "follower_count", "poster_name", "tag_list", "is_image_or_video", "image_src", "save_location", "category"]
pin_df_cleaned = pin_df_cleaned.select(desired_column_order)

# Ensure the "ind" column is of integer type for consistency or analysis purposes
pin_df_cleaned = pin_df_cleaned.withColumn("ind", col("ind").cast("int"))

# COMMAND ----------

# Import necessary data types from pyspark.sql.types
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# Define the schema for the data structure of the incoming geo stream
geo_schema = StructType([
    StructField("ind", StringType()),  # Unique index for each geo record
    StructField("timestamp", StringType()),  # Timestamp of the geo event
    StructField("latitude", DoubleType()),  # Latitude coordinate of the geo event
    StructField("longitude", DoubleType()),  # Longitude coordinate of the geo event
    StructField("country", StringType())  # Country where the geo event occurred
])

# Apply the defined schema to the streaming DataFrame by casting the data as STRING
# and then using from_json to apply the schema, extracting the structured data
geo_df = geo_stream_df \
    .selectExpr("CAST(data as STRING)") \
    .select(from_json(col("data"), geo_schema).alias("geo_data")) \
    .select("geo_data.*")  # Flatten the structure to directly access fields

# COMMAND ----------

# Combine latitude and longitude into a single column named "coordinates" using an array structure
geo_df_cleaned = geo_df.withColumn("coordinates", array(col("latitude"), col("longitude")))

# Drop the original "latitude" and "longitude" columns as they are now redundant
geo_df_cleaned = geo_df_cleaned.drop("latitude", "longitude")

# Convert the "timestamp" column to a timestamp data type to enable time-based operations
geo_df_cleaned = geo_df_cleaned.withColumn("timestamp", to_timestamp(col("timestamp")))

# Select and reorder the columns to "ind", "country", "coordinates", and "timestamp" for a cleaner DataFrame structure
geo_df_cleaned = geo_df_cleaned.select("ind", "country", "coordinates", "timestamp")

# COMMAND ----------

# Define the schema for the data structure of the incoming user stream
user_schema = StructType([
    StructField("ind", StringType()),  # Unique index for each user record
    StructField("first_name", StringType()),  # First name of the user
    StructField("last_name", StringType()),  # Last name of the user
    StructField("age", IntegerType()),  # Age of the user
    StructField("date_joined", TimestampType())  # Timestamp when the user joined
])

# Apply the defined schema to the streaming DataFrame by casting the raw data as STRING
# and then using from_json to apply the schema, extracting the structured data
user_df = user_stream_df \
    .selectExpr("CAST(data as STRING)") \
    .select(from_json(col("data"), user_schema).alias("user_data")) \
    .select("user_data.*")  # Flatten the structure to directly access fields

# COMMAND ----------

# Combine "first_name" and "last_name" into a single "user_name" column using a space as the separator
user_df_cleaned = user_df.withColumn("user_name", concat_ws(" ", col("first_name"), col("last_name")))

# Drop the now redundant "first_name" and "last_name" columns after combining them into "user_name"
user_df_cleaned = user_df_cleaned.drop("first_name", "last_name")

# Ensure "date_joined" column is in the correct timestamp format for consistency and further time-based analysis
user_df_cleaned = user_df_cleaned.withColumn("date_joined", to_timestamp(col("date_joined"), "yyyy-MM-dd'T'HH:mm:ss"))

# Select and reorder columns to "ind", "user_name", "age", "date_joined" for a cleaner DataFrame structure
user_df_cleaned = user_df_cleaned.select("ind", "user_name", "age", "date_joined")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Task 6

# COMMAND ----------

# Write the cleaned pin DataFrame to a Delta table in append mode
pin_df_cleaned.writeStream \
  .format("delta") \  # Specify the output format as Delta, optimized for Spark and Databricks
  .outputMode("append") \  # Use append mode to add new records to the table without modifying existing data
  .option("checkpointLocation", "/tmp/kinesis/_checkpoints/") \  # Set the checkpoint location for fault tolerance
  .table("124f8314c0a1_pin_table")  # Specify the name of the Delta table to write the data to

# COMMAND ----------

# Write the cleaned geo DataFrame to a Delta table in append mode
geo_df_cleaned.writeStream \
  .format("delta") \  # Specify the output format as Delta, optimized for Spark and Databricks
  .outputMode("append") \  # Use append mode to add new records to the table without modifying existing data
  .option("checkpointLocation", "/tmp/kinesis/_checkpoints/") \  # Set the checkpoint location for fault tolerance
  .table("124f8314c0a1_geo_table")  # Specify the name of the Delta table to write the data to

# COMMAND ----------

# Write the cleaned user DataFrame to a Delta table in append mode
user_df_cleaned.writeStream \
  .format("delta") \  # Specify the output format as Delta, optimized for Spark and Databricks
  .outputMode("append") \  # Use append mode to add new records to the table without modifying existing data
  .option("checkpointLocation", "/tmp/kinesis/_checkpoints/") \  # Set the checkpoint location for fault tolerance
  .table("124f8314c0a1_user_table")  # Specify the name of the Delta table to write the data to
