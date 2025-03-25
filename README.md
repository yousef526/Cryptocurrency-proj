# Cryptocurrency Data Pipeline

This project establishes a streamlined data pipeline for ingesting cryptocurrency data from an external API, processing it, and storing the results in a PostgreSQL database. The workflow utilizes Kafka for data streaming, PySpark for processing, and Apache Airflow for orchestration.

## Infrastructure

The solution leverages Docker Compose to set up the following services:

- **PostgreSQL**: Serves as the storage backend for processed data.
- **Zookeeper**: Manages the Kafka cluster.
- **Kafka**: Handles the streaming of data.
- **Kafdrop**: Provides a web UI for monitoring Kafka topics.
- **Apache Airflow**: Orchestrates the data pipeline.

## Setup Instructions

1. **Prepare the Directory Structure**:
   - Create a new directory for the project.
   - Inside this directory, create three subdirectories: `dags`, `plugins`, and `logs`.

2. **Deploy Services with Docker Compose**:
   - Navigate to the project directory.
   - Execute the following command to start the services:
     ```bash
     docker-compose up -d
     ```

3. **Run the Main DAG in Airflow**:
   - Access the Airflow web interface.
   - Trigger the main DAG to initiate the data pipeline.

## Data Pipeline Overview

1. **Data Ingestion**:
   - Fetches cryptocurrency data from an external API.
   - Publishes the retrieved data to a Kafka topic using a Kafka producer.

2. **Data Processing**:
   - Consumes the data from the Kafka topic using PySpark.
   - Performs necessary transformations and processing on the data.

3. **Data Storage**:
   - Stores the processed data into the PostgreSQL database for further analysis and querying.

## Monitoring

- **Kafdrop**:
  - Provides a web-based interface to monitor and manage Kafka topics, brokers, and consumers.
  - Accessible through the URL specified in the Docker Compose configuration.

- **Airflow UI**:
  - Allows tracking and managing DAG runs, tasks, and overall workflow status.
  - Accessible through the URL specified in the Docker Compose configuration.

## Dependencies

- **Docker** and **Docker Compose**: Ensure both are installed on your system to deploy the services seamlessly.
- **Python Packages**: The necessary Python dependencies are specified in the project files and are handled within the Docker containers.

## Notes

- Ensure that the external API endpoint for cryptocurrency data is accessible and that you have the necessary permissions or API keys if required.
- Adjust configuration settings in the Docker Compose file and Airflow DAGs as needed to suit your environment and requirements.

For more detailed information and updates, refer to the project repository: [Cryptocurrency Data Pipeline](https://github.com/yousef526/Cryptocurrency-proj).
