"""
Airflow DAGs to load raw CSV files from Google Cloud Storage (GCS) into BigQuery.

The raw layer stores unprocessed data ingested from external CSV files located 
in a GCS bucket. These files represent various football-related entities 
such as clubs, players, player_valuations, competitions, games and transfers.

Each DAG in this file loads a specific CSV file into its corresponding table
within the `raw` dataset in BigQuery. The loading is done using Airflow's
GCSToBigQueryOperator with automatic schema detection and overwriting behavior.

Requirements:
- Airflow environment with access to GCS.
- Environment variables should define GCP credentials, project configuration and bucket name.
"""

import os
from airflow import DAG
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.utils.dates import days_ago

# Environment variables for GCS and BigQuery configuration
BUCKET_NAME = os.getenv("BUCKET_NAME")
PROJECT_ID = os.getenv("PROJECT_ID")
DATASET_ID = "raw"
GCS_FILE_PATH = "raw_data"

# === DAG: Load clubs.csv into raw.clubs_raw ===
with DAG(
    dag_id="load_clubs_raw_to_bigquery",
    default_args={"owner": "airflow"},
    schedule_interval=None,
    start_date=days_ago(1),
    tags=["gcs", "blobs"],
) as dag:

    load_clubs_csv_to_bigquery = GCSToBigQueryOperator(
        task_id='load_clubs_to_bigquery',
        bucket=BUCKET_NAME,
        source_objects=f'{GCS_FILE_PATH}/clubs.csv',
        destination_project_dataset_table=f'{PROJECT_ID}.{DATASET_ID}.clubs_raw',
        autodetect=True,
        write_disposition='WRITE_TRUNCATE',
        source_format='csv',
        skip_leading_rows=1,
    )

    load_clubs_csv_to_bigquery



# === DAG: Load appearances.csv into raw.appearances_raw ===
with DAG(
    dag_id="load_appearances_raw_to_bigquery",
    default_args={"owner": "airflow"},
    schedule_interval=None,
    start_date=days_ago(1),
    tags=["gcs", "blobs"],
) as dag:

    load_appearances_csv_to_bigquery = GCSToBigQueryOperator(
        task_id='load_appearances_to_bigquery',
        bucket=BUCKET_NAME,
        source_objects=f'{GCS_FILE_PATH}/appearances.csv',
        destination_project_dataset_table=f'{PROJECT_ID}.{DATASET_ID}.appearances_raw',
        autodetect=True,
        write_disposition='WRITE_TRUNCATE',
        source_format='csv',
        skip_leading_rows=1,
    )

    load_appearances_csv_to_bigquery


# === DAG: Load club_games.csv into raw.club_games_raw ===
with DAG(
    dag_id="load_club_games_raw_to_bigquery",
    default_args={"owner": "airflow"},
    schedule_interval=None,
    start_date=days_ago(1),
    tags=["gcs", "blobs"],
) as dag:

    load_club_games_raw_to_bigquery = GCSToBigQueryOperator(
        task_id='load_club_games_raw_to_bigquery',
        bucket=BUCKET_NAME,
        source_objects=f'{GCS_FILE_PATH}/club_games.csv',
        destination_project_dataset_table=f'{PROJECT_ID}.{DATASET_ID}.club_games_raw',
        autodetect=True,
        write_disposition='WRITE_TRUNCATE',
        source_format='csv',
        skip_leading_rows=1,
    )

    load_club_games_raw_to_bigquery


# === DAG: Load competitions.csv into raw.competitions_raw ===
with DAG(
    dag_id="load_competitions_raw_to_bigquery",
    default_args={"owner": "airflow"},
    schedule_interval=None,
    start_date=days_ago(1),
    tags=["gcs", "blobs"],
) as dag:

    load_competitions_raw_to_bigquery = GCSToBigQueryOperator(
        task_id='load_competitions_raw_to_bigquery',
        bucket=BUCKET_NAME,
        source_objects=f'{GCS_FILE_PATH}/competitions.csv',
        destination_project_dataset_table=f'{PROJECT_ID}.{DATASET_ID}.competitions_raw',
        autodetect=True,
        write_disposition='WRITE_TRUNCATE',
        source_format='csv',
        skip_leading_rows=1,
    )

    load_competitions_raw_to_bigquery


# === DAG: Load game_events.csv into raw.game_events_raw ===
with DAG(
    dag_id="load_game_events_raw_to_bigquery",
    default_args={"owner": "airflow"},
    schedule_interval=None,
    start_date=days_ago(1),
    tags=["gcs", "blobs"],
) as dag:

    load_game_events_raw_to_bigquery = GCSToBigQueryOperator(
        task_id='load_game_events_raw_to_bigquery',
        bucket=BUCKET_NAME,
        source_objects=f'{GCS_FILE_PATH}/game_events.csv',
        destination_project_dataset_table=f'{PROJECT_ID}.{DATASET_ID}.game_events_raw',
        autodetect=True,
        write_disposition='WRITE_TRUNCATE',
        source_format='csv',
        skip_leading_rows=1,
    )

    load_game_events_raw_to_bigquery


# === DAG: Load game_lineups.csv into raw.game_lineups_raw ===
with DAG(
    dag_id="load_game_lineups_raw_to_bigquery",
    default_args={"owner": "airflow"},
    schedule_interval=None,
    start_date=days_ago(1),
    tags=["gcs", "blobs"],
) as dag:

    load_game_lineups_raw_to_bigquery = GCSToBigQueryOperator(
        task_id='load_game_lineups_raw_to_bigquery',
        bucket=BUCKET_NAME,
        source_objects=f'{GCS_FILE_PATH}/game_lineups.csv',
        destination_project_dataset_table=f'{PROJECT_ID}.{DATASET_ID}.game_lineups_raw',
        autodetect=True,
        write_disposition='WRITE_TRUNCATE',
        source_format='csv',
        skip_leading_rows=1,
    )

    load_game_lineups_raw_to_bigquery



# === DAG: Load games.csv into raw.games_raw ===
with DAG(
    dag_id="load_games_raw_to_bigquery",
    default_args={"owner": "airflow"},
    schedule_interval=None,
    start_date=days_ago(1),
    tags=["gcs", "blobs"],
) as dag:

    load_games_raw_to_bigquery = GCSToBigQueryOperator(
        task_id='load_games_raw_to_bigquery',
        bucket=BUCKET_NAME,
        source_objects=f'{GCS_FILE_PATH}/games.csv',
        destination_project_dataset_table=f'{PROJECT_ID}.{DATASET_ID}.games_raw',
        autodetect=True,
        write_disposition='WRITE_TRUNCATE',
        source_format='csv',
        skip_leading_rows=1,
    )

    load_games_raw_to_bigquery


# === DAG: Load player_valuations.csv into raw.player_valuations_raw ===
with DAG(
    dag_id="load_player_valuations_raw_to_bigquery",
    default_args={"owner": "airflow"},
    schedule_interval=None,
    start_date=days_ago(1),
    tags=["gcs", "blobs"],
) as dag:

    load_player_valuations_raw_to_bigquery = GCSToBigQueryOperator(
        task_id='load_player_valuations_raw_to_bigquery',
        bucket=BUCKET_NAME,
        source_objects=f'{GCS_FILE_PATH}/player_valuations.csv',
        destination_project_dataset_table=f'{PROJECT_ID}.{DATASET_ID}.player_valuations_raw',
        autodetect=True,
        write_disposition='WRITE_TRUNCATE',
        source_format='csv',
        skip_leading_rows=1,
    )

    load_player_valuations_raw_to_bigquery


# === DAG: Load players.csv into raw.players_raw ===
with DAG(
    dag_id="load_players_raw_to_bigquery",
    default_args={"owner": "airflow"},
    schedule_interval=None,
    start_date=days_ago(1),
    tags=["gcs", "blobs"],
) as dag:

    load_players_raw_to_bigquery = GCSToBigQueryOperator(
        task_id='load_players_raw_to_bigquery',
        bucket=BUCKET_NAME,
        source_objects=f'{GCS_FILE_PATH}/players.csv',
        destination_project_dataset_table=f'{PROJECT_ID}.{DATASET_ID}.players_raw',
        autodetect=True,
        write_disposition='WRITE_TRUNCATE',
        source_format='csv',
        skip_leading_rows=1,
    )

    load_players_raw_to_bigquery


# === DAG: Load transfers.csv into raw.transfers_raw ===
with DAG(
    dag_id="load_transfers_raw_to_bigquery",
    default_args={"owner": "airflow"},
    schedule_interval=None,
    start_date=days_ago(1),
    tags=["gcs", "blobs"],
) as dag:

    load_transfers_raw_to_bigquery = GCSToBigQueryOperator(
        task_id='load_transfers_raw_to_bigquery',
        bucket=BUCKET_NAME,
        source_objects=f'{GCS_FILE_PATH}/transfers.csv',
        destination_project_dataset_table=f'{PROJECT_ID}.{DATASET_ID}.transfers_raw',
        autodetect=True,
        write_disposition='WRITE_TRUNCATE',
        source_format='csv',
        skip_leading_rows=1,
    )

    load_transfers_raw_to_bigquery
