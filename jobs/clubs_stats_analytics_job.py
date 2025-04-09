"""
ETL job to compute seasonal club statistics from games and club data, and load the results into the 'analytics' layer in BigQuery.

This script is a component of the data pipeline executed with Apache Airflow. It reads curated data from the 
'stage' layer in BigQuery, performs aggregations related to club performance (matches played, goals, results, 
win percentage, etc.), and stores the results in the 'analytics' dataset.

Requirements:
    - PySpark with the BigQuery and GCS connectors.
    - Environment variables properly set:
        * TEMP_BUCKET_NAME
        * CONNECTOR_FILE
        * PROJECT_ID
        * CREDENTIALS_FILE
    - The following staging tables must exist in BigQuery:
        * stage.clubs_stage
        * stage.games_stage

Output:
    - Seasonal performance statistics per club are written to: `analytics.clubs_stats`
"""

import os
from pyspark.sql import SparkSession


TEMP_BUCKET_NAME = os.getenv("TEMP_BUCKET_NAME")
CONNECTOR_FILE = os.getenv("CONNECTOR_FILE")
PROJECT_ID = os.getenv("PROJECT_ID")
CREDENTIALS_FILE = os.getenv("CREDENTIALS_FILE")


def proccess_clubs_stats():
    """
    Extracts, calculates, and loads season-level club statistics from the stage layer to the analytics layer in BigQuery.

    Steps:
        1. Initialize a Spark session with the required GCP and BigQuery configurations.
        2. Load data from staging tables: clubs and games.
        3. Register Spark DataFrames as temporary views.
        4. Run a SQL query to calculate:
            - Matches played per season
            - Goals scored and conceded (home, away, total)
            - Matches won, drawn, and lost (home and away)
            - Win percentage
        5. Save the resulting aggregated dataset to the `analytics.clubs_stats` table in BigQuery.

    Logs:
        - Confirms successful data load to BigQuery.
    """


    # Initialize Spark Session
    spark = SparkSession.builder \
                        .appName("clubs-stats-analytics") \
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
    
    games_df = spark.read \
                    .format("bigquery") \
                    .option("table", f"{PROJECT_ID}.stage.games_stage") \
                    .load()
    
    # Register dataframes as temporary tables
    clubs_df.createOrReplaceTempView("clubs_stage")
    games_df.createOrReplaceTempView("games_stage")

    # SQL Query
    query = """
    SELECT
        c.club_id,
        c.name AS club_name,
        g.season,
        COUNT(*) as total_matches,

        -- goals scored
        SUM (CASE WHEN g.away_club_id = c.club_id THEN g.away_club_goals ELSE 0 END) AS goals_scored_as_away,
        SUM (CASE WHEN g.home_club_id = c.club_id THEN g.home_club_goals ELSE 0 END) AS goals_scored_as_home,
        SUM (CASE
                WHEN g.away_club_id = c.club_id THEN g.away_club_goals
                WHEN g.home_club_id = c.club_id THEN g.home_club_goals
                ELSE 0
            END) AS total_goals_scored,

        -- goals conceded
        SUM (CASE WHEN g.home_club_id = c.club_id THEN g.away_club_goals ELSE 0 END) AS goals_conceded_as_away,
        SUM (CASE WHEN g.away_club_id = c.club_id THEN g.home_club_goals ELSE 0 END) AS goals_conceded_as_home,
        SUM (CASE
                WHEN g.home_club_id = c.club_id THEN g.away_club_goals
                WHEN g.away_club_id = c.club_id THEN g.home_club_goals
                ELSE 0
            END) AS total_goals_conceded,
        
        -- win counts
        SUM (CASE WHEN g.home_club_id = c.club_id AND g.home_club_goals > g.away_club_goals THEN 1 ELSE 0 END) AS matches_won_as_home,
        SUM (CASE WHEN g.away_club_id = c.club_id AND g.away_club_goals > g.home_club_goals THEN 1 ELSE 0 END) AS matches_won_as_away,

        -- draw counts
        SUM (CASE WHEN g.home_club_id = c.club_id AND g.away_club_goals = g.home_club_goals THEN 1 ELSE 0 END) AS matches_draw_as_home,
        SUM (CASE WHEN g.away_club_id = c.club_id AND g.away_club_goals = g.home_club_goals THEN 1 ELSE 0 END) AS matches_draw_as_away,

        -- lose counts
        SUM (CASE WHEN g.home_club_id = c.club_id AND g.away_club_goals > g.home_club_goals THEN 1 ELSE 0 END) AS matches_lost_as_home,
        SUM (CASE WHEN g.away_club_id = c.club_id AND g.home_club_goals > g.away_club_goals THEN 1 ELSE 0 END) as matches_lost_as_away,

        -- win percentage
        ROUND(
            SUM(CASE
                WHEN g.home_club_id = c.club_id AND g.home_club_goals > g.away_club_goals THEN 1
                WHEN g.away_club_id = c.club_id AND g.away_club_goals > g.home_club_goals THEN 1
                ELSE 0
            END)/
            COUNT(CASE
                WHEN g.home_club_id = c.club_id THEN 1
                WHEN g.away_club_id = c.club_id THEN 1
            END)
        ,2) AS win_percentage_total

    FROM clubs_stage AS c
    JOIN games_stage AS g
    ON g.away_club_id = c.club_id OR g.home_club_id = c.club_id
    GROUP BY c.club_id, c.name, g.season
    """

    # Execute SQL Query
    clubs_stats_df = spark.sql(query)

    # Write the dataframe on BigQuery
    clubs_stats_df.write \
                .mode("overwrite") \
                .format("bigquery") \
                .option("project", PROJECT_ID) \
                .option("table", f"{PROJECT_ID}.analytics.clubs_stats") \
                .save()
    
    print("✅ clubs_stats analytics table created successfully!")

    spark.stop()
