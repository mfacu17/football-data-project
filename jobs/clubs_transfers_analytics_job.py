"""
ETL job to analyze transfer activity by club using staging tables and write results to the 'analytics' layer in BigQuery.

This script is part of the data processing pipeline executed within Apache Airflow. It reads curated data 
from the 'stage' layer in BigQuery, calculates aggregated transfer metrics per club (e.g., total sales, acquisitions, 
net balance, national vs foreign players), and writes the final analytics table to the 'analytics' dataset.

Requirements:
    - PySpark with the BigQuery and GCS connectors.
    - Environment variables properly set:
        * TEMP_BUCKET_NAME
        * CONNECTOR_FILE
        * PROJECT_ID
        * CREDENTIALS_FILE
    - The following staging tables must exist in BigQuery:
        * stage.clubs_stage
        * stage.players_stage
        * stage.competitions_stage
        * stage.transfers_stage

Output:
    - Aggregated club-level transfers data is written to: `analytics.clubs_transfers`
"""

import os
from pyspark.sql import SparkSession


TEMP_BUCKET_NAME = os.getenv("TEMP_BUCKET_NAME")
CONNECTOR_FILE = os.getenv("CONNECTOR_FILE")
PROJECT_ID = os.getenv("PROJECT_ID")
CREDENTIALS_FILE = os.getenv("CREDENTIALS_FILE")


def proccess_clubs_transfers():
    """
    Extracts, aggregates, and loads transfer data by club from the stage layer to the analytics layer in BigQuery.

    Steps:
        1. Create a Spark session with the necessary GCP configurations.
        2. Read staging tables: clubs, players, competitions, and transfers from BigQuery.
        3. Register temporary views for SQL processing.
        4. Compute club-level metrics using SQL:
            - Total transfer sales and acquisitions
            - Net transfer balance (sales - acquisitions)
            - Number of national vs. foreign players per club
        5. Write the resulting dataset to the 'analytics.clubs_transfers' table in BigQuery.

    Logs:
        - Confirms successful data load to BigQuery.
    """

    # Initialize Spark Session
    spark = SparkSession.builder \
                        .appName("club-transfers-analytics") \
                        .config("spark.jars", f"{CONNECTOR_FILE}") \
                        .config("spark.jars.packages", "com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.12") \
                        .config("spkar.bigquery.proect", f"{PROJECT_ID}") \
                        .config("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
                        .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", f"{CREDENTIALS_FILE}") \
                        .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
                        .getOrCreate()
    
    # Spark needs a temporary bucket as a previous instance to create a new table on BigQuery
    spark.conf.set("temporaryGcsBucket", TEMP_BUCKET_NAME)

    # Read data from BigQuery
    clubs_df = spark.read \
                    .format("bigquery") \
                    .option("table", f"{PROJECT_ID}.stage.clubs_stage") \
                    .load()
    
    players_df = spark.read \
                    .format("bigquery") \
                    .option("table", f"{PROJECT_ID}.stage.players_stage") \
                    .load()
    
    competitions_df = spark.read \
                    .format("bigquery") \
                    .option("table", f"{PROJECT_ID}.stage.competitions_stage") \
                    .load()
    
    transfers_df = spark.read \
                    .format("bigquery") \
                    .option("table", f"{PROJECT_ID}.stage.transfers_stage") \
                    .load()

    # Register dataframes as temporary tables
    clubs_df.createOrReplaceTempView("clubs_stage")
    players_df.createOrReplaceTempView("players_stage")
    competitions_df.createOrReplaceTempView("competitions_stage")
    transfers_df.createOrReplaceTempView("transfers_stage")

    # SQL Query
    query = """
    WITH club_transfers AS (
        SELECT
            c.club_id,
            c.name AS club_name,
            cm.country_name,
            SUM (CASE WHEN t.from_club_name = c.name THEN t.transfer_fee ELSE 0 END) AS total_sales,
            SUM (CASE WHEN t.to_club_name = c.name THEN t.transfer_fee ELSE 0 END) AS total_acquisitions,
            SUM (CASE WHEN t.from_club_name = c.name THEN t.transfer_fee ELSE 0 END) - 
            SUM (CASE WHEN t.to_club_name = c.name THEN t.transfer_fee ELSE 0 END) AS net_transfers
        FROM clubs_stage AS c
        JOIN transfers_stage AS t
        ON t.from_club_name = c.name OR t.to_club_name = c.name
        JOIN competitions_stage AS cm
        ON c.domestic_competition_id = cm.competition_id
        WHERE t.transfer_fee > 0
        GROUP BY c.club_id, c.name, cm.country_name
    )

    SELECT
        ct.club_id,
        ct.club_name,
        SUM (CASE WHEN p.country_of_citizenship = ct.country_name THEN 1 ELSE 0 END) AS national_players_count,
        SUM (CASE WHEN P.country_of_citizenship != ct.country_name THEN 1 ELSE 0 END) AS foreign_players_count,
        ct.total_sales,
        ct.total_acquisitions,
        ct.net_transfers
    FROM players_stage AS p
    JOIN club_transfers AS ct
    ON p.current_club_id = ct.club_id
    GROUP BY ct.club_id, ct.club_name, ct.country_name, ct.total_sales, ct.total_acquisitions, ct.net_transfers
    """

    # Execute SQL Query
    clubs_transfers_df = spark.sql(query)

    # Write dataframe on BigQuery
    clubs_transfers_df.write \
                     .mode("overwrite") \
                     .format("bigquery") \
                     .option("project", PROJECT_ID) \
                     .option("table", f"{PROJECT_ID}.analytics.clubs_transfers") \
                     .save()
    
    print("✅ clubs_transfers analytics table created successfully!")

    spark.stop()