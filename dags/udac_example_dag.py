from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator, PostgresOperator)
from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    # https://airflow.apache.org/docs/apache-airflow/1.10.1/tutorial.html
    # https://airflow.apache.org/docs/apache-airflow/stable/_api/airflow/models/dag/index.html 
    'owner': 'udacity',
    'start_date': datetime(2021, 1, 12,0,0,0,0),
    'end_date': datetime(2021,1,12,4,0,0,0),
    'depends_on_past' : False, 
    'retries' : 3,
    'retry_delay' : timedelta(minutes=5),
    'catchup': False,  # Perform scheduler catchup (or only run latest)
    'max_active_runs': 1  # maximum number of active DAG runs
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='@hourly'
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

#create_tables = PostgresOperator(
    # Runs all the create table statments writen in the create_table.sql 
    # https://marclamberti.com/blog/postgres-operator-airflow/
#    task_id='create_tables',
#    dag=dag,
#    postgres_conn_id="redshift",
#    sql="create_tables.sql"
#)

stage_events_to_redshift = StageToRedshiftOperator(
    # loads all the info from log_data json to staging_events table 
    task_id='stage_events',
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3bucket = "s3bucket",
    s3path = "log_data",
    table = "staging_events",
    region = "us-west-2",
    json = "s3://udacity-dend/log_data",
    dag=dag
)

stage_songs_to_redshift = StageToRedshiftOperator(
    # loads data from song_data json to staging_songs table
    task_id='stage_songs',
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3bucket = "s3bucket",
    s3path = "song_data",
    table = "staging_songs",
    region = "us-west-2",
    json = "s3://udacity-dend/song_data/A/A/A",
    dag=dag
)

load_songplays_table = LoadFactOperator(
    # takes information for staging_events & staging_songs table to insert data into songplays table
    task_id='load_songplays_fact_table',
    postgres_conn_id="redshift",
    target_table = "songplays",
    sql=SqlQueries.songplay_table_insert,
    dag=dag
)

load_user_dimension_table = LoadDimensionOperator(
    # takes information for staging_events & staging_songs table to insert data into users table
    task_id='Load_user_dim_table',
    postgres_conn_id="redshift",
    sql=SqlQueries.user_table_insert,
    target_table = "users",
    append = False,
    dag=dag
)

load_song_dimension_table = LoadDimensionOperator(
    # takes information for staging_events & staging_songs table to insert data into songs table
    task_id='Load_song_dim_table',
    postgres_conn_id="redshift",
    sql=SqlQueries.song_table_insert,
    target_table = "songs",
    append = False,
    dag=dag
)

load_artist_dimension_table = LoadDimensionOperator(
    # takes information for staging_events & staging_songs table to insert data into artists table
    task_id='Load_artist_dim_table',
    postgres_conn_id="redshift",
    sql=SqlQueries.artist_table_insert,
    target_table = "artists",
    append = False, 
    dag=dag
)

load_time_dimension_table = LoadDimensionOperator(
    # takes information for staging_events & staging_songs table to insert data into time table
    task_id='Load_time_dim_table',
    postgres_conn_id="redshift",
    sql=SqlQueries.time_table_insert,
    target_table = "time",
    append = False, 
    dag=dag
)
table_list = ["artists", "songplays", "users", "songs", "time"]
run_quality_checks = DataQualityOperator(
    # checks that there is data in users, artists, songs, songplays, & time table 
    task_id='Run_data_quality_checks',
    redshift_conn_id = "redshift",
    #test = [f"SELECT count(*) from {table}" for table in table_list],
    test = "Select count(*) from songs where songid is null;",
    expected_result = 0,
    #aws_credentials_id = "aws_credentials",
    dag=dag
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

# DAG Order 
#start_operator >> create_tables
#create_tables >>  stage_events_to_redshift
#create_tables >>  stage_songs_to_redshift
start_operator >>  stage_events_to_redshift
start_operator >>  stage_songs_to_redshift
stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table
load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table
load_time_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_user_dimension_table >> run_quality_checks
load_song_dimension_table >> run_quality_checks
run_quality_checks >> end_operator

