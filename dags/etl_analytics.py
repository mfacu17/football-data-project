"""
Airflow DAGs to load curated analytics tables into BigQuery.

Each DAG triggers a specific Python job located in the "jobs" module,
which performs the transformation and loading of data into BigQuery.

These DAGs are designed to be triggered manually and perform the final
processing step before the data is made available for consumption.

Requirements:
    - Python callable jobs for each analytics table must be implemented under `jobs/`.
    - GCP credentials and project ID properly set in the environment for BigQuery and GCS access.
    - BigQuery datasets for the analytics layer must already exist.
"""

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
import sys
import os

# Add the project's root directory to the Python path to enable imports from the 'jobs' module
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

# Import job functions for each analytics table
from jobs.transfers_summary_analytics_job import proccess_transfers
from jobs.players_stats_analytics_job import proccess_players
from jobs.clubs_transfers_analytics_job import proccess_clubs_transfers
from jobs.clubs_stats_analytics_job import proccess_clubs_stats

# DAG to load the transfers summary analytics table into BigQuery
with DAG(
    dag_id='transfers_summary_to_bigquery',
    default_args={'owner': 'airflow'},
    schedule_interval=None,
    start_date=days_ago(1),
    tags=['gcs', 'bigquery']
) as dag:
    load_transfers_summary_analytics = PythonOperator(
        task_id="proccess_transfers_summary",
        python_callable=proccess_transfers
    )
    load_transfers_summary_analytics


# DAG to load player statistics into the analytics layer in BigQuery
with DAG(
    dag_id='player_stats_to_bigquery',
    default_args={'owner': 'airflow'},
    schedule_interval=None,
    start_date=days_ago(1),
    tags=['gcs', 'bigquery']
) as dag:
    load_player_stats_analytics = PythonOperator(
        task_id="proccess_player_stats",
        python_callable=proccess_players
    )
    load_player_stats_analytics


# DAG to load club-level transfer analytics into BigQuery
with DAG(
    dag_id='clubs_transfers_to_bigquery',
    default_args={'owner': 'airflow'},
    schedule_interval=None,
    start_date=days_ago(1),
    tags=['gcs', 'bigquery']
) as dag:
    load_clubs_transfers_analytics = PythonOperator(
        task_id="proccess_clubs_transfers",
        python_callable=proccess_clubs_transfers
    )
    load_clubs_transfers_analytics


# DAG to load club performance statistics into the analytics layer in BigQuery
with DAG(
    dag_id='clubs_stats_to_bigquery',
    default_args={'owner': 'airflow'},
    schedule_interval=None,
    start_date=days_ago(1),
    tags=['gcs', 'bigquery']
) as dag:
    load_clubs_stats_analytics = PythonOperator(
        task_id="proccess_clubs_stats",
        python_callable=proccess_clubs_stats
    )
    load_clubs_stats_analytics
