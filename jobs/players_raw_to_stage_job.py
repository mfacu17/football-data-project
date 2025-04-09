"""
ETL job to clean and load players from the 'raw' layer to the 'stage' layer in BigQuery.

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
    - The 'raw.players_raw' table must exist in BigQuery.
    - GCS bucket configured to store rejected records.

Outputs:
    - Cleaned data is written to: `stage.players_stage`
    - Invalid rows are saved to GCS: `gs://<BUCKET_NAME>/errors/players_stage_errors/`
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col


BUCKET_NAME = os.getenv("BUCKET_NAME")
TEMP_BUCKET_NAME = os.getenv("TEMP_BUCKET_NAME")
PROJECT_ID = os.getenv("PROJECT_ID")
CREDENTIALS_FILE = os.getenv("CREDENTIALS_FILE")
CONNECTOR_FILE = os.getenv("CONNECTOR_FILE")


def players_raw_to_stage():
    """
    Extracts, validates, and loads 'players' data from the raw layer to the stage layer in BigQuery.

    Steps:
        1. Create a Spark session with the necessary GCP configurations.
        2. Read the 'players_raw' table from the 'raw' dataset in BigQuery.
        3. Identify and save records with nulls in key columns to a GCS path for traceability.
        4. Remove rows with null values in critical columns and drop duplicate records.
        5. Fill missing values in selected non-critical columns with default values:
            - 'market_value_in_eur', 'height_in_cm' → 0
            - 'sub_position', 'foot', 'country_of_birth' → 'Unknown'
            - 'contract_expiration_date' → "2099-12-31 23:59:59"
        6. Drop non-essential columns 'name', 'player_code', 'city_of_birth', 'agent_name', 'image_url', 'url', 'current_club_domestic_competition_id', 'current_club_name', 'highest_maker_value_in_eur'
        7. Load the cleaned dataset into the 'stage.players_stage' table in BigQuery.

    Logs:
        - Logs the number of null records found.
        - Confirms successful data load to BigQuery.
    """

    # Create Spark session
    spark = SparkSession.builder \
                        .appName("players-raw-to-stage") \
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
        .option("table", "players_raw") \
        .load()
    
    # FInd nulls registers in key columns
    df_nulls = df.filter(
        col("player_id").isNull() |
        col("first_name").isNull() |
        col("last_name").isNull() |
        col("country_of_citizenship").isNull() |
        col("current_club_id").isNull()
    )

    null_count = df_nulls.count()
    print(f"📌 Null registers: {null_count}")

    # Save nulls
    if null_count > 0:
        df_nulls.write.mode("overwrite").csv(
            f"gs://{BUCKET_NAME}/errors/players_stage_errors",
            header=True
        )
    
    # Eliminate nulls and duplicates
    df_clean = df.dropna(subset=["player_id", "first_name", "last_name", "country_of_citizenship", "current_club_id"]) \
                .dropDuplicates(["player_id"]) \
                .fillna({
                    "market_value_in_eur": 0,
                    "height_in_cm": 0,
                    "sub_position": 'Unknown',
                    "foot": 'Unknown',
                    "country_of_birth": 'Unknown',
                    "contract_expiration_date": "2099-12-31 23:59:59"
                }) \
                .withColumn("contract_expiration_date", col("contract_expiration_date").cast("timestamp"))

    # Drop columns 'name', 'player_code', 'city_of_birth', 'agent_name', 'image_url', 'url', 'current_club_domestic_competition_id', 'current_club_name', 'highest_maker_value_in_eur'
    df_clean = df_clean.drop("name", "player_code", "city_of_birth", "agent_name", "image_url", "url", "current_club_domestic_competition_id", "current_club_name", "highest_market_value_in_eur")

    # Load cleaned data to BigQuery
    df_clean.write.mode("overwrite").format("bigquery") \
                .option("project", PROJECT_ID) \
                .option("table", f"{PROJECT_ID}.stage.players_stage") \
                .save()
    
    print("✅ players stage table loaded successfully.")

    spark.stop()