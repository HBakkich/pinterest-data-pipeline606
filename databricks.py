# Databricks notebook source
# MAGIC %md
# MAGIC ## Milestone 6

# COMMAND ----------

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
# dbutils.fs.unmount(MOUNT_NAME)

# Mount the drive
dbutils.fs.mount(SOURCE_URL, MOUNT_NAME)

# COMMAND ----------

display(dbutils.fs.ls(MOUNT_NAME))

# For Milestone 8, comment out all code above until this point, 
#  otherwise the Apache Airflow Dag run will fail


# COMMAND ----------

# MAGIC %sql
# MAGIC -- Disable format checks during the reading of Delta tables
# MAGIC SET spark.databricks.delta.formatCheck.enabled=false

# COMMAND ----------

# File location and type
# Asterisk(*) indicates reading all the content of the specified file that have .json extension
pin_file_location = "/mnt/pinterest-data/topics/124f8314c0a1.pin/partition=0//*.json"
geo_file_location = "/mnt/pinterest-data/topics/124f8314c0a1.geo/partition=0//*.json" 
user_file_location = "/mnt/pinterest-data/topics/124f8314c0a1.user/partition=0//*.json"

# Read in pin JSON from mounted S3 bucket
pin_df = spark.read.format("json").option("inferSchema", "true").load(pin_file_location)
# Read in geo JSON from mounted S3 bucket
geo_df = spark.read.format("json").option("inferSchema", "true").load(geo_file_location)
# Read in user JSON from mounted S3 bucket
user_df = spark.read.format("json").option("inferSchema", "true").load(user_file_location)

# COMMAND ----------

# Display the Spark dataframes to check their contents
print("pin_df: \n")
pin_df.printSchema()
display(pin_df.limit(10))

print("\ngeo_df: \n")
geo_df.printSchema()
display(geo_df.limit(10))

print("\nuser_df: \n")
user_df.printSchema()
display(user_df.limit(10))


# COMMAND ----------

# MAGIC %md
# MAGIC ## Milestone 7

# COMMAND ----------

# MAGIC %md
# MAGIC #### Task 1

# COMMAND ----------


# Replace empty entries and entries with no relevant data with None
for column in pin_df.columns:
    pin_df = pin_df.withColumn(column, col(column).cast("string"))
    pin_df = pin_df.withColumn(column, 
                               when(col(column).isin(["", " ", "NULL", "null", "None", "none"]), None)
                               .otherwise(col(column)))

# Convert follower_count to numbers and ensure its data type is an integer
pin_df = pin_df.withColumn("follower_count", regexp_extract(col("follower_count"), "\d+", 0).cast("int"))


# Clean the data in the save_location column to include only the save location path

pin_df = pin_df.withColumn("save_location", regexp_extract(col("save_location"), "Your_Regex_Pattern_Here", 0))

# Rename the index column to ind
pin_df = pin_df.withColumnRenamed("index", "ind")

# Reorder the DataFrame columns
pin_df = pin_df.select("ind", "unique_id", "title", "description", "follower_count", 
                       "poster_name", "tag_list", "is_image_or_video", "image_src", 
                       "save_location", "category")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Task 2

# COMMAND ----------


# Create a new column 'coordinates' that contains an array based on the latitude and longitude columns
geo_df = geo_df.withColumn("coordinates", struct(col("latitude"), col("longitude")))

# Drop the latitude and longitude columns from the DataFrame
geo_df = geo_df.drop("latitude", "longitude")

# Convert the timestamp column from a string to a timestamp data type
geo_df = geo_df.withColumn("timestamp", to_timestamp(col("timestamp")))

# Reorder the DataFrame columns
geo_df = geo_df.select("ind", "country", "coordinates", "timestamp")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Task 3

# COMMAND ----------


# Create a new column 'user_name' that concatenates the information found in the first_name and last_name columns
user_df = user_df.withColumn("user_name", concat_ws(" ", col("first_name"), col("last_name")))

# Drop the first_name and last_name columns from the DataFrame
user_df = user_df.drop("first_name", "last_name")

# Convert the date_joined column from a string to a timestamp data type
user_df = user_df.withColumn("date_joined", to_timestamp(col("date_joined")))

# Convert the age column from a string to an integer data type
user_df = user_df.withColumn("age", col("age").cast("int"))

# Reorder the DataFrame columns
user_df = user_df.select("ind", "user_name", "age", "date_joined")

# COMMAND ----------

# Get columns to find out which dataframes to query
print("pin_df: \n")
pin_df.printSchema()
print("geo_df: \n")
geo_df.printSchema()
print("user_df: \n")
user_df.printSchema()


# COMMAND ----------

# MAGIC %md
# MAGIC #### Task 4

# COMMAND ----------

from pyspark.sql.window import Window

# Join geo_df and pin_df on the 'ind' column
joined_df = geo_df.join(pin_df, "ind")

# Group by country and category and count the occurrences
category_counts = joined_df.groupBy("country", "category").agg(count("category").alias("category_count"))

# Define a window partitioned by country, ordered by category_count descending
windowSpec = Window.partitionBy("country").orderBy(desc("category_count"))

# Use the window spec to add a row number, then filter to get the most popular category per country
most_popular_category = category_counts.withColumn("row_number", row_number().over(windowSpec)).filter(col("row_number") == 1).drop("row_number")

# Select the desired columns
result_df = most_popular_category.select("country", "category", "category_count")

display(result_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Task 5

# COMMAND ----------



# First, filter the DataFrame for the years 2018 to 2022
filtered_df = joined_df.filter(year("timestamp").between(2018, 2022))

# Extract the year from the timestamp column and group by the extracted year and category
category_yearly_counts = filtered_df.withColumn("post_year", year(col("timestamp"))).groupBy("post_year", "category").agg(count("category").alias("category_count"))

# Order the results by post_year and category_count
ordered_category_counts = category_yearly_counts.orderBy("post_year", col("category_count").desc())

display(ordered_category_counts)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Task 6

# COMMAND ----------


# Step1:
# Define a window partitioned by country and ordered by follower_count descending
windowSpec = Window.partitionBy("country").orderBy(col("follower_count").desc())

# Add a row number to each row within the window to rank the dataframe
ranked_df = joined_df.withColumn("row_number", row_number().over(windowSpec))

# Filter to keep only the user with the most followers per country 
most_followed_users = ranked_df.filter(col("row_number") == 1)

# Select the desired columns
result_step1 = most_followed_users.select("country", "poster_name", "follower_count")


# COMMAND ----------

# Step 2:
# Group by country and find the maximum follower_count
max_follower_country = result_step1.groupBy("country").agg(max("follower_count").alias("follower_count"))

# Order the results by follower_count descending
result_step2 = max_follower_country.orderBy(col("follower_count").desc())

# Limit the result to only one entry
result_step2 = result_step2.limit(1).select("country", "follower_count")

display(result_step2)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Task 7

# COMMAND ----------


# Join user_df with posts_df on user_id
joined_df = user_df.join(pin_df, user_df.ind == pin_df.ind)

# Categorize ages into groups
joined_df = joined_df.withColumn("age_group", 
                                 when(col("age").between(18, 24), "18-24")
                                 .when(col("age").between(25, 35), "25-35")
                                 .when(col("age").between(36, 50), "36-50")
                                 .otherwise("+50"))

# Group by age group and category, then count posts
category_counts = joined_df.groupBy("age_group", "category").agg(count("category").alias("category_count"))

# Rank categories within each age group based on count
windowSpec = Window.partitionBy("age_group").orderBy(col("category_count").desc())
ranked_categories = category_counts.withColumn("rank", row_number().over(windowSpec))

# Filter for top categories in each age group
top_categories = ranked_categories.filter(col("rank") == 1).select("age_group", "category", "category_count")

display(top_categories)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Task 8

# COMMAND ----------


# Join user_df with pin_df to associate ages with follower counts
joined_df = user_df.join(pin_df, user_df.ind == pin_df.ind)

# Categorize ages into groups in the joined DataFrame
joined_df = joined_df.withColumn("age_group", 
                                 when(col("age").between(18, 24), "18-24")
                                 .when(col("age").between(25, 35), "25-35")
                                 .when(col("age").between(36, 50), "36-50")
                                 .otherwise("+50"))

# Calculate the median follower count for each age group
median_follower_count_df = joined_df.groupBy("age_group") \
    .agg(expr("percentile_approx(follower_count, 0.5)").alias("median_follower_count"))

display(median_follower_count_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Task 9

# COMMAND ----------


# Extract year from date_joined and filter between 2015 and 2020
users_joined = user_df.withColumn("post_year", year("date_joined")).filter((col("post_year") >= 2015) & (col("post_year") <= 2020))

# Group by the extracted year and count the number of users
users_joined_count = users_joined.groupBy("post_year").agg(count("*").alias("number_users_joined"))

display(users_joined_count)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Task 10

# COMMAND ----------


# Join user_df with pin_df to get follower counts
joined_df = user_df.join(pin_df, user_df.ind == pin_df.ind)

# Extract year from date_joined, filter between 2015 and 2020, and calculate median follower count
median_follower_count_df = joined_df.withColumn("post_year", year("date_joined")) \
    .filter((col("post_year") >= 2015) & (col("post_year") <= 2020)) \
    .groupBy("post_year") \
    .agg(expr("percentile_approx(follower_count, 0.5)").alias("median_follower_count"))

display(median_follower_count_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Task 11

# COMMAND ----------


# Join user_df with pin_df to get follower counts
joined_df = user_df.join(pin_df, user_df.ind == pin_df.ind)

# Filter users who joined between 2015 and 2020
filtered_users = joined_df.withColumn("post_year", year(col("date_joined"))) \
                          .filter((col("post_year") >= 2015) & (col("post_year") <= 2020))

# Categorize ages into groups
processed_df = filtered_users.withColumn("age_group", 
                                         when(col("age").between(18, 24), "18-24")
                                         .when(col("age").between(25, 35), "25-35")
                                         .when(col("age").between(36, 50), "36-50")
                                         .otherwise("+50"))

# Calculate the median follower count for each age group and post_year
median_follower_count_df = processed_df.groupBy("age_group", "post_year") \
    .agg(expr("percentile_approx(follower_count, 0.5)").alias("median_follower_count"))

display(median_follower_count_df)
