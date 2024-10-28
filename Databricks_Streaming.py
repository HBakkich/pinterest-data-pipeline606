# Databricks notebook source
# Import necessary data types from pyspark.sql.types
import Databricks_Utils
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType

# COMMAND ----------

# MAGIC %run DataBricks_Auth

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

pin_stream_df = read_stream_df('streaming-124f8314c0a1-pin')
# Uncomment the below line to display the streaming data in the notebook
# display(pin_stream_df)

# COMMAND ----------

# Setting up a streaming DataFrame to read data from AWS Kinesis for geographical data
geo_stream_df = read_stream_df('streaming-124f8314c0a1-geo')

# Uncomment to view streaming data
# display(geo_stream_df)

# COMMAND ----------

# Setting up a streaming DataFrame to read data from AWS Kinesis for user data
user_stream_df = read_stream_df('streaming-124f8314c0a1-user')

# Uncomment to view streaming data
# display(user_stream_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Task 5

# COMMAND ----------

# Apply the defined schema (in Databricks_Schemas notebook) to the streaming DataFrames by casting the data as STRING
# and then using from_json to apply the schema, extracting the structured data

# COMMAND ----------

# MAGIC %run Databricks_Schemas

# COMMAND ----------


# Convert the streaming DataFrame, which is in JSON format, to a DataFrame with the defined schema
pin_df = convert_df(pin_stream_df, pin_schema, "pin_data")

geo_df = convert_df(geo_stream_df, geo_schema, "geo_data")

user_df = convert_df(user_stream_df, user_schema, "user_data") 


# clean the pin DataFrame
pin_df_cleaned = clean_pin_data(pin_df)
# Clean the geo DataFrame
geo_df_cleaned = clean_geo_data(geo_df)
# Clean the user DataFrame
user_df_cleaned = clean_user_data(user_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Task 6

# COMMAND ----------


# Write the cleaned pin DataFrame to a Delta table in append mode
write_data_to_delta_table(pin_df_cleaned, "124f8314c0a1_pin_table")
write_data_to_delta_table(geo_df_cleaned, "124f8314c0a1_geo_table")
write_data_to_delta_table(user_df_cleaned, "124f8314c0a1_user_table")
