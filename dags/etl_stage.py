"""
ETL Pipeline for Stage Layer - Airflow DAG Definitions

This module defines a set of Airflow DAGs that orchestrate the transformation 
of raw data from the raw layer into curated datasets for the stage layer. 
Each DAG invokes a PySpark job that reads raw data, Applies quality checks and 
transformations, and writes the cleaned results to BigQuery. 
Invalid or malformed records are written back to GCS for traceability.

This is an intermediate step in preparing the data to be 
modeled and analyzed in the analytics layer.

Structure:
- Each DAG represents a distinct domain entity (e.g., players, clubs, transfers).
- Tasks are implemented as PythonOperators calling PySpark jobs.
- DAGs are manually triggered (`schedule_interval=None`) for development/testing.
- DAG tags are included for filtering in the Airflow UI.

Requirements:
- PySpark jobs must be implemented and available in the `jobs` package.
- Airflow environment with access to GCS and BigQuery.
- Environment variables should define GCP credentials and project configuration.
"""

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
import sys
import os

# Add the project's root directory to the Python path to enable imports from the 'jobs' module
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

# Import PySpark job callables
from jobs.appearances_raw_to_stage_job import appearances_raw_to_stage
from jobs.players_raw_to_stage_job import players_raw_to_stage
from jobs.clubs_raw_to_stage_job import clubs_raw_to_stage
from jobs.competitions_raw_to_stage_job import competitions_raw_to_stage
from jobs.game_events_raw_to_stage_job import game_events_raw_to_stage
from jobs.club_games_raw_to_stage_job import club_games_raw_to_stage
from jobs.game_lineups_raw_to_stage_job import game_lineups_raw_to_stage
from jobs.games_raw_to_stage_job import games_raw_to_stage
from jobs.player_valuations_raw_to_stage_job import player_valuations_raw_to_stage
from jobs.transfers_raw_to_stage_job import transfers_raw_to_stage

# === DAG: Load stage.appearances_stage ===
with DAG(
    dag_id='appearances_stage_to_bigquery',
    default_args={'owner':'airflow'},
    schedule_interval=None,
    start_date=days_ago(1),
    tags=["gcs", "stage", "bigquery"]
) as dag:
    
    # Task to proccess data
    load_appearances_raw_data = PythonOperator(
        task_id="process_appearances_raw_to_stage",
        python_callable=appearances_raw_to_stage
    )

    load_appearances_raw_data

# === DAG: Load stage.players_stage ===
with DAG(
    dag_id='players_stage_to_bigquery',
    default_args={'owner':'airflow'},
    schedule_interval=None,
    start_date=days_ago(1),
    tags=["gcs", "stage", "bigquery"]
) as dag:
    
    # Task to proccess data
    load_players_raw_data = PythonOperator(
        task_id="process_players_raw_to_stage",
        python_callable=players_raw_to_stage
    )

    load_players_raw_data

# === DAG: Load stage.clubs_stage ===
with DAG(
    dag_id='clubs_stage_to_bigquery',
    default_args={'owner':'airflow'},
    schedule_interval=None,
    start_date=days_ago(1),
    tags=["gcs", "stage", "bigquery"]
) as dag:
    
    # Task to proccess data
    load_clubs_raw_data = PythonOperator(
        task_id="process_clubs_raw_to_stage",
        python_callable=clubs_raw_to_stage
    )

    load_clubs_raw_data

# === DAG: Load stage.competitions_stage ===
with DAG(
    dag_id='competitions_stage_to_bigquery',
    default_args={'owner':'airflow'},
    schedule_interval=None,
    start_date=days_ago(1),
    tags=["gcs", "stage", "bigquery"]
) as dag:
    
    # Task to proccess data
    load_competitions_raw_data = PythonOperator(
        task_id="proccess_competitions_raw_to_stage",
        python_callable=competitions_raw_to_stage
    )

    load_competitions_raw_data

# === DAG: Load stage.game_events_stage ===
with DAG(
    dag_id='game_events_stage_to_bigquery',
    default_args={'owner':'airflow'},
    schedule_interval=None,
    start_date=days_ago(1),
    tags=["gcs", "stage", "bigquery"]
) as dag:
    
    # Task to proccess data
    load_game_events_raw_data = PythonOperator(
        task_id="proccess_game_events_raw_to_stage",
        python_callable=game_events_raw_to_stage
    )

    load_game_events_raw_data

# === DAG: Load stage.club_games_stage ===
with DAG(
    dag_id='club_games_stage_to_bigquery',
    default_args={'owner':'airflow'},
    schedule_interval=None,
    start_date=days_ago(1),
    tags=["gcs", "stage", "bigquery"]
) as dag:
    
    # Task to proccess data
    load_club_games_raw_data = PythonOperator(
        task_id="proccess_club_games_raw_to_stage",
        python_callable=club_games_raw_to_stage
    )

    load_club_games_raw_data

# === DAG: Load stage.game_lineups_stage ===
with DAG(
    dag_id='game_lineups_stage_to_bigquery',
    default_args={'owner':'airflow'},
    schedule_interval=None,
    start_date=days_ago(1),
    tags=["gcs", "stage", "bigquery"]
) as dag:
    
    # Task to proccess data
    load_game_lineups_raw_data = PythonOperator(
        task_id="proccess_game_lineups_raw_to_stage",
        python_callable=game_lineups_raw_to_stage
    )

    load_game_lineups_raw_data

# === DAG: Load stage.games_stage ===
with DAG(
    dag_id='games_stage_to_bigquery',
    default_args={'owner':'airflow'},
    schedule_interval=None,
    start_date=days_ago(1),
    tags=["gcs", "stage", "bigquery"]
) as dag:
    
    # Task to proccess data
    load_games_raw_data = PythonOperator(
        task_id="proccess_games_raw_to_stage",
        python_callable=games_raw_to_stage
    )

    load_games_raw_data

# === DAG: Load stage.player_valuations_stage ===
with DAG(
    dag_id='player_valuations_stage_to_bigquery',
    default_args={'owner':'airflow'},
    schedule_interval=None,
    start_date=days_ago(1),
    tags=["gcs", "stage", "bigquery"]
) as dag:
    
    # Task to proccess data
    load_player_valuations_raw_data = PythonOperator(
        task_id="proccess_player_valuations_to_stage",
        python_callable=player_valuations_raw_to_stage
    )

    load_player_valuations_raw_data

# === DAG: Load stage.transfers_stage ===
with DAG(
    dag_id='transfers_stage_to_bigquery',
    default_args={'owner':'airflow'},
    schedule_interval=None,
    start_date=days_ago(1),
    tags=["gcs", "stage", "bigquery"]
) as dag:
    
    # Task to proccess data
    load_transfers_raw_data = PythonOperator(
        task_id="proccess_transfers_to_stage",
        python_callable=transfers_raw_to_stage
    )

    load_transfers_raw_data