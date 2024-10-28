# Databricks notebook source
from pyspark.sql.functions import when, col, regexp_replace, concat_ws, array, to_timestamp
from pyspark.sql.types import IntegerType, StringType, StructType, StructField, DoubleType, TimestampType
from pyspark.sql import DataFrame

# COMMAND ----------

# Setting up a streaming DataFrame to read data from AWS Kinesis
def read_stream_df(streamName: str):
    return spark \
    .readStream \
    .format('kinesis') \
    .option('streamName', streamName) \  # Name of the Kinesis stream
    .option('initialPosition','earliest') \  # Start reading from the earliest data record
    .option('region','us-east-1') \  # AWS region where the Kinesis stream is located
    .option('awsAccessKey', ACCESS_KEY) \  # AWS access key for authentication
    .option('awsSecretKey', SECRET_KEY) \  # AWS secret key for authentication
    .load()

# COMMAND ----------


def clean_pin_data(pin_df):
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

    return pin_df_cleaned

# COMMAND ----------

# Combine latitude and longitude into a single column named "coordinates" using an array structure
def clean_geo_data(geo_df):
    geo_df_cleaned = geo_df.withColumn("coordinates", array(col("latitude"), col("longitude")))

    # Drop the original "latitude" and "longitude" columns as they are now redundant
    geo_df_cleaned = geo_df_cleaned.drop("latitude", "longitude")

    # Convert the "timestamp" column to a timestamp data type to enable time-based operations
    geo_df_cleaned = geo_df_cleaned.withColumn("timestamp", to_timestamp(col("timestamp")))

    # Select and reorder the columns to "ind", "country", "coordinates", and "timestamp" for a cleaner DataFrame structure
    geo_df_cleaned = geo_df_cleaned.select("ind", "country", "coordinates", "timestamp")

    return geo_df_cleaned

# COMMAND ----------

def clean_user_data(user_df):
    # Combine "first_name" and "last_name" into a single "user_name" column using a space as the separator
    user_df_cleaned = user_df.withColumn("user_name", concat_ws(" ", col("first_name"), col("last_name")))

    # Drop the now redundant "first_name" and "last_name" columns after combining them into "user_name"
    user_df_cleaned = user_df_cleaned.drop("first_name", "last_name")

    # Ensure "date_joined" column is in the correct timestamp format for consistency and further time-based analysis
    user_df_cleaned = user_df_cleaned.withColumn("date_joined", to_timestamp(col("date_joined"), "yyyy-MM-dd'T'HH:mm:ss"))

    # Select and reorder columns to "ind", "user_name", "age", "date_joined" for a cleaner DataFrame structure
    user_df_cleaned = user_df_cleaned.select("ind", "user_name", "age", "date_joined")

    return user_df_cleaned

# COMMAND ----------

def convert_df(stream_df:DataFrame, schema: StructType, data_alias:str) -> Dataframe:
    df = stream_df \
        .selectExpr("CAST(data as STRING)") \ # NOTE converts column of bytes to a string object that can be converted to a DataFrame
        .select(from_json(col("data"), schema).alias(data_alias)) \ # Apply the schema so each row is an a StructType(DataFrame) from your schema 
        .select("f{data_alias}.*")  # Flatten the structure to directly access fields 
        return df

# COMMAND ----------

def write_data_to_delta_table(cleaned_df: DataFrame, table_name: str) -> None:
  cleaned_df.writeStream \
    .format("delta") \  # Specify the output format as Delta, optimized for Spark and Databricks
    .outputMode("append") \  # Use append mode to add new records to the table without modifying existing data
    .option("checkpointLocation", "/tmp/kinesis/_checkpoints/") \  # Set the checkpoint location for fault tolerance
    .table(table_name)  # Specify the name of the Delta table to write the data to