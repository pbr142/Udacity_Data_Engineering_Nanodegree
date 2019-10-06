from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'pbr142',
    'start_date': datetime(2018, 12, 1),
    'end_date': datetime(2018, 12, 31),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(seconds=300),
	'catchup': False,
    'email_on_retry': False
}

dag = DAG('sparkify_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *',
		  max_active_runs=3
)

start_operator = DummyOperator(task_id='Begin_execution', dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
	provide_context=True,
	
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="udacity-dend",
    s3_key="log_data",
    json_format="auto",
	region="us-west-2",
	table="staging_events"
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
	provide_context=True,
	
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="udacity-dend",
    s3_key="song_data",
	json_format="auto",
	region="us-west-2",
	table="staging_songs"
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
	provide_context=True,
	
    redshift_conn_id="redshift",
    table="songplays",
    query=SqlQueries.songplay_table_insert
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
	provide_context=True,
	
    redshift_conn_id="redshift",
    table="users",
    truncate_table=True,
    query=SqlQueries.user_table_insert
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
	provide_context=True,
	
    redshift_conn_id="redshift",
    table="songs",
    truncate_table=True,
    query=SqlQueries.song_table_insert
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
	provide_context=True,
	
    redshift_conn_id="redshift",
    table="artists",
    truncate_table=True,
    query=SqlQueries.artist_table_insert
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
	provide_context=True,
	
    redshift_conn_id="redshift",
    table="time",
    truncate_table=True,
    query=SqlQueries.time_table_insert
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
	provide_context=True,
	
    redshift_conn_id="redshift",
    tables=[
        "songplays",
        "users",
        "songs",
        "artists",
        "time"
    ]
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)



# set order of execution
start_operator >> stage_events_to_redshift
start_operator >> stage_songs_to_redshift

stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table

load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table

load_user_dimension_table >> run_quality_checks
load_song_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks

run_quality_checks >> end_operator
