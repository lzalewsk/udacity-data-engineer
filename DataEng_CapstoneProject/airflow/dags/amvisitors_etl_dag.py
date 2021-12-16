from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator,
                               LoadFactOperator,
                               LoadDimensionOperator,
                               DataQualityOperator,
                               LoadLookupToRedshiftOperator
                              )
from airflow.operators.postgres_operator import PostgresOperator
from helpers import SqlQueries


# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'lukasz.zalewski',
    'depends_on_past': True,
    'catchup_by_default': False,
    'start_date': datetime(2016, 1, 1),
    'end_date': datetime(2016, 12, 31),
    'retries': 0,
    'retry_delay': timedelta(minutes=5)
}


dag = DAG('amvisitors_etl_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='@monthly',
          catchup=True,
          max_active_runs = 1
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

# create_tables_operator = PostgresOperator(
#     task_id = 'Create_Redshift_Tables',
#     dag = dag,    
#     postgres_conn_id= 'redshift',
#     sql = SqlQueries.create_all_talbes,
# )


stage_i94immi_operator = StageToRedshiftOperator(
    task_id='Stage_i94immi_events',
    provide_context=True,
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",    
    table = "staging_i94events",
    sql_init_command = SqlQueries.staging_i94events_table_create,
    copy_options="FORMAT AS PARQUET",
    s3_path = "s3://lzalewsk-capstone/sas_data_full",
)



load_dim_visitor = LoadDimensionOperator(
    task_id='Load_Visitor_dim_table',
    dag = dag,
    redshift_conn_id='redshift',
    table='dim_visitor',
    sql_list = [SqlQueries.dim_visitor_insert],
    remove_old_data = False
)


load_dim_time = LoadDimensionOperator(
    task_id='Load_Time_dim_table',
    dag = dag,
    redshift_conn_id='redshift',
    table='dim_time',
    sql_list = [SqlQueries.dim_arrdate_time_insert,SqlQueries.dim_depdate_time_insert],
    remove_old_data = False
)

load_fact_visit = LoadFactOperator(
    task_id='Load_fact_visit_event_table',
    dag = dag,
    provider_context=True,
    redshift_conn_id='redshift',
    table='fact_visit_event',
    sql = SqlQueries.fact_visit_insert
)

run_quality_checks = DataQualityOperator(
    task_id='Run_Data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    tables=['fact_visit_event','dim_time','dim_visitor']
)


end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)


# DAG definition

dim_tables_tasks = [
    load_dim_visitor,
    load_dim_time
]

start_operator >> stage_i94immi_operator >> dim_tables_tasks >> load_fact_visit >> run_quality_checks >> end_operator