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
    'owner': 'udacity',
    'start_date': datetime(2020, 1, 1),
    #'schedule_interval': '@hourly', 
    'depends_on_past' : False, 
    'retries' : 3,
    'retry_delay' : timedelta(minutes=5)
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          start_date=datetime(2020,1,1,0,0,0,0),
          schedule_interval='@hourly'
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

create_tables_task = PostgresOperator(
    # Runs all the create table statments writen in the create_table.sql 
    task_id='create_tables',
    dag=dag,
    postgres_conn_id="redshift",
    sql='create_tables.sql'
)

stage_events_to_redshift = StageToRedshiftOperator(
    # loads all the info from log_data json to staging_events table 
    task_id='stage_events',
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3bucket = "s3bucket",
    s3path = "log_data",
    table = "staging_events",
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
    json = "s3://udacity-dend/song_data",
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
    dag=dag
)

load_song_dimension_table = LoadDimensionOperator(
    # takes information for staging_events & staging_songs table to insert data into songs table
    task_id='Load_song_dim_table',
    postgres_conn_id="redshift",
    sql=SqlQueries.song_table_insert,
    target_table = "songs",
    dag=dag
)

load_artist_dimension_table = LoadDimensionOperator(
    # takes information for staging_events & staging_songs table to insert data into artists table
    task_id='Load_artist_dim_table',
    postgres_conn_id="redshift",
    sql=SqlQueries.artist_table_insert,
    target_table = "artists",
    dag=dag
)

load_time_dimension_table = LoadDimensionOperator(
    # takes information for staging_events & staging_songs table to insert data into time table
    task_id='Load_time_dim_table',
    postgres_conn_id="redshift",
    sql=SqlQueries.time_table_insert,
    target_table = "time",
    dag=dag
)

run_quality_checks = DataQualityOperator(
    # checks that there is data in users, artists, songs, songplays, & time table 
    task_id='Run_data_quality_checks',
    redshift_conn_id = "redshift",
    #aws_credentials_id = "aws_credentials",
    dag=dag
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> create_tables_task
create_tables_task >>  stage_events_to_redshift
create_tables_task >>  stage_songs_to_redshift
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

