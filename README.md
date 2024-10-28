# Pinterest Data Pipeline Project

This project aims to build a data pipeline similar to Pinterest's system, which processes billions of data points daily. Using AWS services like EC2, MSK (Managed Streaming for Apache Kafka), and S3, you will set up a scalable data processing system. The project also includes stream processing with AWS Kinesis and batch processing with Databricks.

## Table of Contents
1. [Overview](#overview)
2. [Technologies Used](#technologies-used)
3. [Setup Instructions](#setup-instructions)
   - [GitHub Setup](#github-setup)
   - [AWS Setup](#aws-setup)
4. [Milestones](#milestones)
   - [Milestone 1: Environment Setup](#milestone-1-environment-setup)
   - [Milestone 2: Get Started](#milestone-2-get-started)
   - [Milestone 3: Batch Processing - Configure EC2 Kafka Client](#milestone-3-batch-processing-configure-ec2-kafka-client)
   - [Milestone 4: Batch Processing - MSK to S3](#milestone-4-batch-processing-msk-to-s3)
   - [Milestone 5: Batch Processing - API Gateway](#milestone-5-batch-processing-api-gateway)
   - [Milestone 6: Batch Processing - Databricks Setup](#milestone-6-batch-processing-databricks-setup)
   - [Milestone 7: Batch Processing - Spark on Databricks](#milestone-7-batch-processing-spark-on-databricks)
   - [Milestone 8: Batch Processing - AWS MWAA](#milestone-8-batch-processing-aws-mwaa)
   - [Milestone 9: Stream Processing - AWS Kinesis](#milestone-9-stream-processing-aws-kinesis)
5. [File Structure](#file-structure)
6. [Contributing](#contributing)
7. [License](#license)

## Overview
Pinterest uses a large-scale data pipeline to process user-generated data. In this project, you’ll build a similar system using AWS Cloud services for batch and stream processing. The project includes setting up data ingestion using MSK (Kafka), processing using Databricks, and storing processed data in S3.

## Technologies Used
- **AWS EC2** for Kafka client setup
- **AWS MSK** (Managed Streaming for Apache Kafka) for data streaming
- **AWS S3** for data storage
- **AWS API Gateway** for API management
- **AWS MWAA** (Managed Workflows for Apache Airflow) for task orchestration
- **AWS Kinesis** for stream processing
- **Databricks** for batch data processing

## Setup Instructions

### GitHub Setup
1. Create a GitHub repository to track your project progress.
2. Clone the repository to your local machine:
   ```bash
   git clone <repository-url>
   cd <repository-name>

### AWS Setup
1. Create an AWS account and set up the necessary services (EC2, MSK, S3, API Gateway).
2. Ensure you have configured the necessary roles and permissions (IAM, EC2 roles, MSK roles).

## Milestones

### Milestone 1: Environment Setup
- **Task 1: Set up GitHub to track code changes.**
  - Create a new repository and commit your progress regularly.

- **Task 2: Set up AWS to host various services used in the project.**
  - Create an AWS account and ensure services like EC2 and MSK are available.

### Milestone 2: Get Started
- **Task 1: Download and configure the Pinterest infrastructure.**
  - Download the `user_posting_emulation.py` script, which includes access to a simulated RDS database with three tables:
    - `pinterest_data`: Contains information about Pinterest posts.
    - `geolocation_data`: Contains geolocation information for each post.
    - `user_data`: Contains user information for each post.
  - Create a `db_creds.yaml` file to store the database credentials (HOST, USER, PASSWORD). Add this file to `.gitignore` to avoid uploading sensitive information.
  - Run the script and print `pin_result`, `geo_result`, and `user_result` to become familiar with the data.

- **Task 2: Sign in to AWS.**
  - Use the credentials provided to log in to AWS and ensure you're in the `us-east-1` region.

### Milestone 3: Batch Processing - Configure EC2 Kafka Client
- **Task 1: Create a `.pem` key file in AWS Parameter Store.**
  - Create a key pair and save it locally as `KeyName.pem`.

- **Task 2: Connect to EC2.**
  ```bash
  ssh -i "KeyName.pem" ec2-user@<instance-public-ip>

- **Task 3: Install Kafka on the EC2 instance.**
  - Install the specific version of Kafka (`2.12-2.8.1`):
    ```bash
    wget https://archive.apache.org/dist/kafka/2.8.1/kafka_2.12-2.8.1.tgz
    tar -xvzf kafka_2.12-2.8.1.tgz
    ```
  - Install the IAM MSK authentication package.

- **Task 4: Create Kafka topics.**
  - Use the MSK Console to retrieve the Bootstrap servers and Zookeeper connection strings.
  - Create the following Kafka topics:
    - `<your_UserId>.pin`
    - `<your_UserId>.geo`
    - `<your_UserId>.user`
    ```bash
    kafka-topics.sh --create --topic <your_UserId>.pin --bootstrap-server <BootstrapServerString> --partitions 3 --replication-factor 2
    ```

### Milestone 4: Batch Processing - MSK to S3
- **Task 1: Create a custom plugin in MSK Connect.**
  - Use Confluent.io's Amazon S3 Connector to create a plugin that connects Kafka to S3.

- **Task 2: Build a connector to store Kafka data into S3.**
  - Configure the plugin to store data from the Kafka topics into the specified S3 bucket using the following regex for `topics.regex`: `<your_UserId>.*`.

### Milestone 5: Batch Processing - API Gateway
- **Task 1: Create an API using API Gateway.**
  - Set up a REST API with Kafka REST Proxy as the integration method.

- **Task 2: Set up Kafka REST Proxy on EC2.**
  - Install the Confluent Kafka REST Proxy package on the EC2 instance.

- **Task 3: Send data to the API and check it reaches the MSK cluster.**
  - Modify `user_posting_emulation.py` to send data to the API using the API's `Invoke URL`. Run a Kafka consumer to verify data ingestion:
    ```bash
    kafka-console-consumer.sh --topic <your_UserId>.pin --bootstrap-server <BootstrapServerString>
    ```

### Milestone 6: Batch Processing - Databricks Setup
- **Task 1: Set up a Databricks account and access the workspace.**

- **Task 2: Mount the S3 bucket in Databricks.**
  - Mount the S3 bucket where the data from Kafka is stored:
    ```python
    dbutils.fs.mount("s3://<bucket-name>", "/mnt/<mount-name>")
    ```
  - Create DataFrames for the data.

### Milestone 7: Batch Processing - Spark on Databricks
- **Task 1: Clean and transform the `df_pin`, `df_geo`, and `df_user` DataFrames using Spark.**
  - Example transformation:
    ```python
    df_pin = df_pin.withColumnRenamed("index", "ind").withColumn("follower_count", df_pin["follower_count"].cast("int"))
    ```

- **Task 2: Perform queries to find insights.**
  - Example query to find the most popular category by country:
    ```python
    df_popular_category = df_pin.groupBy("country", "category").count().withColumnRenamed("count", "category_count").orderBy("category_count", ascending=False)
    ```

### Milestone 8: Batch Processing - AWS MWAA
- **Task 1: Create and upload a DAG to MWAA.**
  - Write a DAG that triggers a Databricks notebook job and upload it to the MWAA environment.

### Milestone 9: Stream Processing - AWS Kinesis
- **Task 1: Create Kinesis data streams.**
  - Create three Kinesis streams for the data:
    - `streaming-<your_UserId>-pin`
    - `streaming-<your_UserId>-geo`
    - `streaming-<your_UserId>-user`

- **Task 2: Configure API Gateway to integrate with Kinesis.**

- **Task 3: Send data to the Kinesis streams.**
  - Modify `user_posting_emulation.py` to send streaming data to Kinesis.

- **Task 4: Read data from Kinesis streams in Databricks.**
  - Use a Databricks notebook to consume and process the streaming data.

## File Structure
```bash
.
├── README.md
├── user_posting_emulation.py   # Script to simulate Pinterest user posts
├── db_creds.yaml               # YAML file storing database credentials (not uploaded)
├── kafka_client.properties     # Configuration file for Kafka client
└── dags/
    └── <your_UserId_dag.py>    # Airflow DAG for orchestrating Databricks jobs
```
## Contributing
Feel free to fork this repository and create pull requests. Contributions should follow the standard GitHub flow (fork, branch, commit, PR).
## License

This project is licensed under the MIT License - see the LICENSE file for details.
