"""
ETL job to summarize player transfer history and load the result into the 'analytics' layer in BigQuery.

This script aggregates player transfer data to provide a historical view of all transfers per player,
including transfer fees, origin/destination clubs, and the player's most expensive transfer.

Requirements:
    - PySpark with the BigQuery and GCS connectors.
    - Environment variables properly set:
        * TEMP_BUCKET_NAME
        * CONNECTOR_FILE
        * PROJECT_ID
        * CREDENTIALS_FILE
    - The following staging tables must exist in BigQuery:
        * stage.players_stage
        * stage.transfers_stage

Output:
    - Transfer summary table written to: `analytics.transfers_summary`
"""

import os
from pyspark.sql import SparkSession


TEMP_BUCKET_NAME = os.getenv("TEMP_BUCKET_NAME")
CONNECTOR_FILE = os.getenv("CONNECTOR_FILE")
PROJECT_ID = os.getenv("PROJECT_ID")
CREDENTIALS_FILE = os.getenv("CREDENTIALS_FILE")


def proccess_transfers():
    """
    Extracts, transforms, and loads a summary of player transfers into the analytics layer of BigQuery.

    Steps:
        1. Initialize a Spark session with required GCP and BigQuery configurations.
        2. Load player and transfer staging tables from BigQuery.
        3. Register Spark DataFrames as temporary views.
        4. Execute a SQL query that:
            - Joins player and transfer data.
            - Adds player names and computes the maximum transfer fee per player.
            - Orders the data for better traceability (by player and date).
        5. Save the resulting transfer summary to the `analytics.transfers_summary` table.

    Logs:
        - Confirms successful data load to BigQuery.
    """


    # Initialize Spark session
    spark = SparkSession.builder \
                        .appName("transfers-summary-analytics") \
                        .config("spark.jars", f"{CONNECTOR_FILE}") \
                        .config("spark.jars.packages", "com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.12") \
                        .config("spark.bigquery.project", f"{PROJECT_ID}") \
                        .config("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
                        .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", f"{CREDENTIALS_FILE}") \
                        .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
                        .getOrCreate()
    
    # Spark needs a temporary bucket as a previous instance to create a new table on BigQuery
    spark.conf.set("temporaryGcsBucket",TEMP_BUCKET_NAME)

    # Read data from BigQuery
    players_df = spark.read \
                    .format("bigquery") \
                    .option("table", f"{PROJECT_ID}.stage.players_stage") \
                    .load()                        
    
    transfers_df = spark.read \
                    .format("bigquery") \
                    .option("table", f"{PROJECT_ID}.stage.transfers_stage") \
                    .load()
    
    # Register dataframes as temporary tables
    players_df.createOrReplaceTempView("players_stage")
    transfers_df.createOrReplaceTempView("transfers_stage")

    # SQL Query
    query = """
        WITH transfers_summary AS (
            SELECT
                t.player_id,
                CONCAT(p.first_name,' ', p.last_name) AS player_name,
                t.transfer_date,
                t.from_club_name AS from_club,
                t.to_club_name AS to_club,
                t.transfer_fee,
                MAX(t.transfer_fee) OVER (PARTITION BY t.player_id) AS max_transfer_fee
            FROM transfers_stage as t
            JOIN players_stage as p
            ON t.player_id = p.player_id
            ORDER BY t.player_id ASC, t.transfer_date DESC
        )
        SELECT *
        FROM transfers_summary
    """

    # Execute SQL Query
    transfers_summary__df = spark.sql(query)

    # Write the dataframe on BigQuery
    transfers_summary__df.write \
                    .mode("overwrite") \
                    .format("bigquery") \
                    .option("project", PROJECT_ID) \
                    .option("table", f"{PROJECT_ID}.analytics.transfers_summary") \
                    .save()
    
    print("✅ transfers_summary analytics table created successfully!")

    spark.stop()