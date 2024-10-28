# Databricks notebook source
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType

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

# Define the schema for the data structure of the incoming geo stream
geo_schema = StructType([
    StructField("ind", StringType()),  # Unique index for each geo record
    StructField("timestamp", StringType()),  # Timestamp of the geo event
    StructField("latitude", DoubleType()),  # Latitude coordinate of the geo event
    StructField("longitude", DoubleType()),  # Longitude coordinate of the geo event
    StructField("country", StringType())  # Country where the geo event occurred
])

# Define the schema for the data structure of the incoming user stream
user_schema = StructType([
    StructField("ind", StringType()),  # Unique index for each user record
    StructField("first_name", StringType()),  # First name of the user
    StructField("last_name", StringType()),  # Last name of the user
    StructField("age", IntegerType()),  # Age of the user
    StructField("date_joined", TimestampType())  # Timestamp when the user joined
])
