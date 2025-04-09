"""
ETL job to clean and load game_events data from the 'raw' layer to the 'stage' layer in BigQuery.

This script is part of the data processing pipeline executed within Apache Airflow. It reads raw data
from BigQuery, performs essential data quality checks (nulls and duplicates), stores any invalid
records for traceability, applies default values to specific nullable fields, and loads the cleaned data into a staging table.

Requirements:
    - PySpark with the BigQuery and GCS connectors.
    - Environment variables properly set:
        * BUCKET_NAME
        * TEMP_BUCKET_NAME
        * PROJECT_ID
        * CREDENTIALS_FILE
        * CONNECTOR_FILE
    - The 'raw.game_events_raw' table must exist in BigQuery.
    - GCS bucket configured to store rejected records.

Outputs:
    - Cleaned data is written to: `stage.game_events_stage`
    - Invalid rows are saved to GCS: `gs://<BUCKET_NAME>/errors/game_events_stage_errors/`
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace


BUCKET_NAME = os.getenv("BUCKET_NAME")
TEMP_BUCKET_NAME = os.getenv("TEMP_BUCKET_NAME")
PROJECT_ID = os.getenv("PROJECT_ID")
CREDENTIALS_FILE = os.getenv("CREDENTIALS_FILE")
CONNECTOR_FILE = os.getenv("CONNECTOR_FILE")


def game_events_raw_to_stage():
    """
    Extracts, validates, and loads 'game_events' data from the raw layer to the stage layer in BigQuery.

    Steps:
        1. Create a Spark session with the necessary GCP configurations.
        2. Read the 'game_events_raw' table from the 'raw' dataset in BigQuery.
        3. Identify and save records with nulls in key columns to a GCS path for traceability.
        4. Remove rows with null values in critical columns and drop duplicate records.
        5. Fill missing values in selected non-critical columns with default values:
            - 'description' → 'N/A'
            - 'player_in_id', 'player_assist_id' → -1
        6. Drop non-essential column 'date'.
        7. Load the cleaned dataset into the 'stage.clubs_stage' table in BigQuery.

    Logs:
        - Logs the number of null records found.
        - Confirms successful data load to BigQuery.
    """

    # Create Spark session
    spark = SparkSession.builder \
                        .appName("Game-events-raw-to-stage") \
                        .config("spark.jars", f"{CONNECTOR_FILE}") \
                        .config("spark.jars.packages", "com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.12") \
                        .config("spark.bigquery.project", PROJECT_ID) \
                        .config("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
                        .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", CREDENTIALS_FILE) \
                        .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
                        .getOrCreate()

    # Spark needs a temporary bucket as a previous instance to create a new table on BigQuery
    spark.conf.set("temporaryGcsBucket",TEMP_BUCKET_NAME)
    
    # Read the dataframe
    df = spark.read.format("bigquery") \
        .option("dataset", "raw") \
        .option("table", "game_events_raw") \
        .load()
    
    # Find nulls registers in key columns
    df_nulls = df.filter(
        col("game_event_id").isNull() |
        col("game_id").isNull() |
        col("club_id").isNull() |
        col("player_id").isNull()
    )

    null_count = df_nulls.count()
    print(f"📌 Null registers: {null_count}")

    # Save nulls
    if null_count > 0:
        df_nulls.write.mode("overwrite").csv(
            f"gs://{BUCKET_NAME}/errors/game_events_stage_errors",
            header=True
        )
    
    # Eliminate nulls and duplicates
    df_clean = df.dropna(subset=["game_event_id", "game_id", "club_id", "player_id"]) \
                .dropDuplicates(["game_event_id"]) \
                .fillna({
                    "description": "N/A",
                    "player_in_id": -1,
                    "player_assist_id": -1
                }) \
                .withColumn("description", regexp_replace(col("description"), r"^,\s*", "")) 
                

    # drop column 'date'
    df_clean = df_clean.drop("date")

    # Load cleaned data to BigQuery
    df_clean.write.mode("overwrite").format("bigquery") \
                .option("project", PROJECT_ID) \
                .option("table", f"{PROJECT_ID}.stage.game_events_stage") \
                .save()
    
    print("✅ game_events stage table loaded successfully.")

    spark.stop()