from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (
    StageToRedshiftOperator,
    LoadFactOperator,
    LoadDimensionOperator,
    DataQualityOperator
)
from helpers import SqlQueries


# -----------------------------
# Default DAG arguments
# -----------------------------
default_args = {
    "owner": "udacity",
    "depends_on_past": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
    "catchup": False,
    "start_date": datetime(2019, 1, 12),
}


# -----------------------------
# Define DAG
# -----------------------------
dag = DAG(
    "s3_to_redshift_dag",
    default_args=default_args,
    description="Load and transform data in Redshift with Airflow",
    schedule_interval="0 * * * *",  # Run every hour at minute 0
)


# -----------------------------
# Tasks
# -----------------------------
start_operator = DummyOperator(
    task_id="Begin_execution",
    dag=dag
)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id="Stage_events",
    dag=dag,
    table="public.staging_events",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="udacity-dend",
    s3_key="log_data",  # Example: log_data/{execution_date.year}
    json="s3://udacity-dend/log_json_path.json"
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id="Stage_songs",
    dag=dag,
    table="public.staging_songs",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="udacity-dend",
    s3_key="song_data/A/A/A",  # Sample path
    json="auto"
)

load_songplays_table = LoadFactOperator(
    task_id="Load_songplays_fact_table",
    dag=dag,
    redshift_conn_id="redshift",
    table="public.songplays",
    truncate_table=True,
    query=SqlQueries.songplay_table_insert
)

load_user_dimension_table = LoadDimensionOperator(
    task_id="Load_user_dim_table",
    dag=dag,
    redshift_conn_id="redshift",
    table="public.users",
    truncate_table=True,
    query=SqlQueries.user_table_insert
)

load_song_dimension_table = LoadDimensionOperator(
    task_id="Load_song_dim_table",
    dag=dag,
    redshift_conn_id="redshift",
    table="public.songs",
    truncate_table=True,
    query=SqlQueries.song_table_insert
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id="Load_artist_dim_table",
    dag=dag,
    redshift_conn_id="redshift",
    table="public.artists",
    truncate_table=True,
    query=SqlQueries.artist_table_insert
)

load_time_dimension_table = LoadDimensionOperator(
    task_id="Load_time_dim_table",
    dag=dag,
    redshift_conn_id="redshift",
    table="public.time",
    truncate_table=True,
    query=SqlQueries.time_table_insert
)


# -----------------------------
# Data Quality Checks
# -----------------------------
dq_checks = [
    {"check_sql": "SELECT COUNT(*) FROM users WHERE userid IS NULL", "expected_result": 0},
    {"check_sql": "SELECT COUNT(*) FROM songs WHERE songid IS NULL", "expected_result": 0},
]

run_quality_checks = DataQualityOperator(
    task_id="Run_data_quality_checks",
    dag=dag,
    redshift_conn_id="redshift",
    dq_checks=dq_checks
)


# -----------------------------
# End Operator
# -----------------------------
end_operator = DummyOperator(
    task_id="Stop_execution",
    dag=dag
)


# -----------------------------
# DAG Dependencies
# -----------------------------
start_operator >> [stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table

load_songplays_table >> [
    load_user_dimension_table,
    load_song_dimension_table,
    load_artist_dimension_table,
    load_time_dimension_table
] >> run_quality_checks >> end_operator
