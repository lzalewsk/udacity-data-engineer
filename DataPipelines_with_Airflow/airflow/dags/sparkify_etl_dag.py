from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from airflow.operators.subdag_operator import SubDagOperator
from schema_creator_subdug import sparkify_schema_creator_dag
from helpers import SqlQueries


# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'lukasz.zalewski',
    'depends_on_past': False,
    'catchup_by_default': False,
    'start_date': datetime(2019, 1, 12),
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
}


dag = DAG('sparkify_etl_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *',
          catchup = False
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)


# DB Scheme creator Subdug
# NOTE: use drop_db_tables_if_exists=True if all Facts and Dimenions Table shall be drop
# 
db_schema_task_id = 'Create_DB_schema'
schema_creator_subdag_task = SubDagOperator(
    subdag=sparkify_schema_creator_dag(
        parent_dag_name = "sparkify_etl_dag",
        task_id = db_schema_task_id,
        redshift_conn_id = "redshift",
        drop_db_tables_if_exists = True,
        default_args=default_args
    ),
    task_id=db_schema_task_id,
    dag=dag
)


stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    provide_context=False,
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",    
    table = "staging_events",
    sql_init_command = SqlQueries.staging_events_table_create,
    copy_options="FORMAT AS JSON 's3://udacity-dend/log_json_path.json'",
    s3_path = "s3://udacity-dend/log_data",
    region="us-west-2"
)


stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    provide_context=False,
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",    
    table = "staging_songs",
    sql_init_command = SqlQueries.staging_songs_table_create,
    copy_options="FORMAT AS JSON 'auto' ACCEPTINVCHARS AS '^'",
    s3_path = "s3://udacity-dend/song_data",
    region="us-west-2"
)


load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id="redshift",
    table='songplays',
    sql=SqlQueries.songplay_table_insert    
)


load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table='users',
    sql=SqlQueries.user_table_insert,
    remove_old_data = True
)


load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table='songs',
    sql=SqlQueries.song_table_insert,
    remove_old_data = True
)


load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table='artists',
    sql=SqlQueries.artist_table_insert,
    remove_old_data = True
)


load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table='time',
    sql=SqlQueries.time_table_insert,
    remove_old_data = True
)


run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    tables=['songplays','users','artists','songs']
)


end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)


# DAG definition
start_operator >> schema_creator_subdag_task
schema_creator_subdag_task >> [stage_events_to_redshift, stage_songs_to_redshift]

[stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table

load_songplays_table >> [load_song_dimension_table,
                         load_user_dimension_table,
                         load_artist_dimension_table,
                         load_time_dimension_table
                        ]
                         
load_song_dimension_table >> run_quality_checks
load_user_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks

run_quality_checks >> end_operator
