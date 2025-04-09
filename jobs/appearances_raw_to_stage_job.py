"""
ETL job to clean and load appearances data from the 'raw' layer to the 'stage' layer in BigQuery.

This script is designed to be executed within an Apache Airflow pipeline. It reads raw data
from BigQuery, performs basic data quality checks (nulls and duplicates), stores any invalid
records for traceability, and loads the cleaned dataset into a staging table for further processing.

Requirements:
    - PySpark with the BigQuery and GCS connectors.
    - Environment variables properly set:
        * BUCKET_NAME
        * TEMP_BUCKET_NAME
        * PROJECT_ID
        * CREDENTIALS_FILE
        * CONNECTOR_FILE
    - The 'raw.appearances_raw' table must exist in BigQuery.
    - GCS bucket configured to store rejected records.

Output:
    - Cleaned data is written to: `stage.appearances_stage` in BigQuery.
    - Invalid rows are saved to GCS: `gs://<BUCKET_NAME>/errors/appearances_stage_errors/`
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col


BUCKET_NAME = os.getenv("BUCKET_NAME")
TEMP_BUCKET_NAME = os.getenv("TEMP_BUCKET_NAME")
PROJECT_ID = os.getenv("PROJECT_ID")
CREDENTIALS_FILE = os.getenv("CREDENTIALS_FILE")
CONNECTOR_FILE = os.getenv("CONNECTOR_FILE")


def appearances_raw_to_stage():
    """
    Extracts, validates, and loads the 'appearances' data from the raw layer to the stage layer in BigQuery.

    Steps:
        1. Create a Spark session with the necessary GCP configurations.
        2. Read the 'appearances_raw' table from the 'raw' dataset in BigQuery.
        3. Identify and save records with nulls in key columns to a GCS path for traceability.
        4. Remove rows with null values in critical columns and drop duplicate records.
        5. Drop non-essential columns like 'player_name' and 'player_current_club_id'.
        6. Write the cleaned DataFrame to the 'stage.appearances_stage' table in BigQuery.

    Logging:
        - Logs the number of null records found.
        - Confirms successful data load to BigQuery.
    """

    spark = SparkSession.builder \
                        .appName("Appearances-raw-to-stage") \
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
        .option("table", "appearances_raw") \
        .load()
    
    # Find nulls registers in key columns
    df_nulls = df.filter(
        col("appearance_id").isNull() |
        col("game_id").isNull() |
        col("player_id").isNull() |
        col("player_club_id").isNull() |
        col("competition_id").isNull()
    )

    null_count = df_nulls.count()
    print(f"📌 Null registers: {null_count}")

    # Save nulls
    if null_count > 0:
        df_nulls.write.mode("overwrite").csv(
            f"gs://{BUCKET_NAME}/errors/appearances_stage_errors",
            header=True
        )
    
    # Eliminate nulls and duplicates
    df_clean = df.dropna(subset=["appearance_id", "game_id", "player_id", "player_club_id", "competition_id"]) \
                .dropDuplicates(["appearance_id"])

    # Drop columns that can be retrieved via foreign keys.
    df_clean = df_clean.drop("player_name", "player_current_club_id")

    # Load cleaned data to BigQuery
    df_clean.write.mode("overwrite").format("bigquery") \
                .option("project", PROJECT_ID) \
                .option("table", f"{PROJECT_ID}.stage.appearances_stage") \
                .save()
    
    print("✅ appearances table loaded successfully.")

    spark.stop()