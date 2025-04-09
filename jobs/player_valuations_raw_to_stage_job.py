"""
ETL job to clean and load player_valuations from the 'raw' layer to the 'stage' layer in BigQuery.

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
    - The 'raw.player_valuations_raw' table must exist in BigQuery.
    - GCS bucket configured to store rejected records.

Outputs:
    - Cleaned data is written to: `stage.player_valuations_stage`
    - Invalid rows are saved to GCS: `gs://<BUCKET_NAME>/errors/player_valuations_stage_errors/`
"""


import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col


BUCKET_NAME = os.getenv("BUCKET_NAME")
TEMP_BUCKET_NAME = os.getenv("TEMP_BUCKET_NAME")
PROJECT_ID = os.getenv("PROJECT_ID")
CREDENTIALS_FILE = os.getenv("CREDENTIALS_FILE")
CONNECTOR_FILE = os.getenv("CONNECTOR_FILE")


def player_valuations_raw_to_stage():
    """
    Extracts, validates, and loads 'player_valuations' data from the raw layer to the stage layer in BigQuery.

    Steps:
        1. Create a Spark session with the necessary GCP configurations.
        2. Read the 'player_valuations_raw' table from the 'raw' dataset in BigQuery.
        3. Identify and save records with nulls in key columns to a GCS path for traceability.
        4. Remove rows with null values in critical columns and drop duplicate records.
        5. Drop non-essential columns 'current_club_id' and 'player_club_domestic_competition_id'
        6. Load the cleaned dataset into the 'stage.player_valuations_stage' table in BigQuery.

    Logs:
        - Logs the number of null records found.
        - Confirms successful data load to BigQuery.
    """

    # Create Spark session
    spark = SparkSession.builder \
                        .appName("player-valuations-raw-to-stage") \
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
        .option("table", "player_valuations_raw") \
        .load()
    
    # FInd nulls registers in key columns
    df_nulls = df.filter(
        col("player_id").isNull() |
        col("date").isNull() |
        col("market_value_in_eur").isNull()
    )

    null_count = df_nulls.count()
    print(f"📌 Null registers: {null_count}")

    # Save nulls
    if null_count > 0:
        df_nulls.write.mode("overwrite").csv(
            f"gs://{BUCKET_NAME}/errors/player_valuations_stage_errors",
            header=True
        )
    
    # Eliminate nulls and duplicates
    df_clean = df.dropna(subset=["player_id", "date", "market_value_in_eur"]) \

    # Drop columns 'current_club_id' and 'player_club_domestic_competition_id'
    df_clean = df_clean.drop("current_club_id", "player_club_domestic_competition_id")

    # Load cleaned data to BigQuery
    df_clean.write.mode("overwrite").format("bigquery") \
                .option("project", PROJECT_ID) \
                .option("table", f"{PROJECT_ID}.stage.player_valuations_stage") \
                .save()
    
    print("✅ player_valuations stage table loaded successfully.")

    spark.stop()