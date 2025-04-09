"""
ETL job to compute player performance statistics per club and season, and load the results into the 'analytics' layer in BigQuery.

This script is part of the data pipeline executed with Apache Airflow. It aggregates player-level statistics 
from appearances data in the 'stage' layer, combining it with club and player metadata. The output includes 
per-club metrics as well as cumulative player career stats.

Requirements:
    - PySpark with the BigQuery and GCS connectors.
    - Environment variables properly set:
        * TEMP_BUCKET_NAME
        * CONNECTOR_FILE
        * PROJECT_ID
        * CREDENTIALS_FILE
    - The following staging tables must exist in BigQuery:
        * stage.appearances_stage
        * stage.clubs_stage
        * stage.players_stage

Output:
    - Player statistics per club and season are written to: `analytics.player_stats`
"""

import os
from pyspark.sql import SparkSession

TEMP_BUCKET_NAME = os.getenv("TEMP_BUCKET_NAME")
CONNECTOR_FILE = os.getenv("CONNECTOR_FILE")
PROJECT_ID = os.getenv("PROJECT_ID")
CREDENTIALS_FILE = os.getenv("CREDENTIALS_FILE")


def proccess_players():
    """
    Extracts, calculates, and loads club-season-level player statistics from the stage layer to the analytics layer in BigQuery.

    Steps:
        1. Initialize a Spark session with required GCP and BigQuery configurations.
        2. Load appearances, clubs, and players staging tables from BigQuery.
        3. Register Spark DataFrames as temporary views.
        4. Run a SQL query to calculate:
            - Goals, assists, cards, minutes, and matches played per player per club per season.
            - Aggregated career stats for each player across all clubs/seasons.
        5. Save the resulting dataset to the `analytics.player_stats` table in BigQuery.

    Logs:
        - Confirms successful data load to BigQuery.
    """

    # Initialize Spark session
    spark = SparkSession.builder \
                        .appName("player-stats-analytics") \
                        .config("spark.jars", f"{CONNECTOR_FILE}") \
                        .config("spark.jars.packages", "com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.12") \
                        .config("spark.bigquery.project", f"{PROJECT_ID}") \
                        .config("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
                        .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", f"{CREDENTIALS_FILE}") \
                        .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
                        .getOrCreate()
    
    # Spark needs a temporary bucket as a previous instance to create a new table on BigQuery
    spark.conf.set("temporaryGcsBucket", TEMP_BUCKET_NAME)

    # Read data from BigQuery
    appearances_df = spark.read \
                        .format("bigquery") \
                        .option("table", f"{PROJECT_ID}.stage.appearances_stage") \
                        .load()
    
    clubs_df = spark.read \
                        .format("bigquery") \
                        .option("table", f"{PROJECT_ID}.stage.clubs_stage") \
                        .load()
    
    players_df = spark.read \
                        .format("bigquery") \
                        .option("table", f"{PROJECT_ID}.stage.players_stage") \
                        .load()
    
    # Register dataframes as temporary tables
    appearances_df.createOrReplaceTempView("appearances_stage")
    clubs_df.createOrReplaceTempView("clubs_stage")
    players_df.createOrReplaceTempView("players_stage")

    # SQL Query
    query = """
    WITH appearances_with_clubs AS (
        SELECT
            a.player_id,
            c.name AS club,
            CONCAT(
                CAST(
                    CASE WHEN MONTH(date) >= 7 THEN YEAR(date) % 100 
                    ELSE (YEAR(date) - 1) % 100 END AS STRING),
                '/',
                CAST(
                    CASE WHEN MONTH(date) >= 7 THEN (YEAR(date) + 1) % 100 
                    ELSE YEAR(date) % 100 END AS STRING)
            ) AS temporada,
            a.goals,
            a.assists,
            a.yellow_cards,
            a.red_cards,
            a.minutes_played
        FROM appearances_stage AS a
        JOIN clubs_stage AS c
        ON a.player_club_id = c.club_id
    )

    SELECT
        ac.player_id,
        ac.club,
        ac.temporada,
        CONCAT(p.first_name, ' ', p.last_name) AS player_name,
        SUM(ac.goals) AS goals_per_club,
        SUM(ac.assists) AS assists_per_club,
        SUM(ac.yellow_cards) AS yellow_cards_per_club,
        SUM(ac.red_cards) AS red_cards_per_club,
        SUM(ac.minutes_played) AS minutes_played_per_club,
        COUNT(*) AS matches_played,
        ROUND(SUM(ac.goals) * 1.0 / NULLIF(COUNT(*), 0), 2) AS avg_goals_per_match,
        SUM(SUM(ac.goals)) OVER (PARTITION BY ac.player_id) AS total_goals,
        SUM(SUM(ac.assists)) OVER (PARTITION BY ac.player_id) AS total_assists,
        SUM(SUM(ac.yellow_cards)) OVER (PARTITION BY ac.player_id) AS total_yellow_cards,
        SUM(SUM(ac.red_cards)) OVER (PARTITION BY ac.player_id) AS total_red_cards,
        SUM(SUM(ac.minutes_played)) OVER (PARTITION BY ac.player_id) AS total_minutes_played
    FROM players_stage AS p
    JOIN appearances_with_clubs AS ac
    ON p.player_id = ac.player_id
    GROUP BY ac.player_id, ac.club, ac.temporada, p.first_name, p.last_name
    """
    
    # Execute SQL Query
    player_stats_df = spark.sql(query)

    # Write dataframe on BigQuery
    player_stats_df.write \
                        .mode("overwrite") \
                        .format("bigquery") \
                        .option("project", PROJECT_ID) \
                        .option("table", f"{PROJECT_ID}.analytics.player_stats") \
                        .save()
    
    print("✅ player_stats analytics table created successfully!")

    spark.stop()