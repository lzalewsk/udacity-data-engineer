from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (DataQualityOperator,
                               LoadLookupToRedshiftOperator)
from airflow.operators.postgres_operator import PostgresOperator
from helpers import SqlQueries


# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'lukasz.zalewski',
    'depends_on_past': False,
    'catchup_by_default': False,
    'start_date': datetime(2021, 4, 12),
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
}


dag = DAG('amvisitors_dbschema_dag',
          default_args=default_args,
          description='Cleand DB and prepare Lookup Tables in Redshift with Airflow',
          schedule_interval=None,
          catchup = False
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)


drop_db_schema = PostgresOperator(
    task_id=f"drop_db_schema",
    dag=dag,
    postgres_conn_id='redshift',
    sql=SqlQueries.db_schema_drop
) 


lt_mode_to_redshift = LoadLookupToRedshiftOperator(
    task_id='Load_Mode_Lookup_Tables',
    provide_context=False,
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",    
    table = "dim_mode",
    sql_init_command = SqlQueries.mode_table_create,
    copy_options="IGNOREHEADER 0 FORMAT AS JSON 'auto'",
    s3_path = "s3://lzalewsk-capstone/lookup_data/_i94model.json",
    region="us-west-2"
)


lt_location_to_redshift = LoadLookupToRedshiftOperator(
    task_id='Load_Location_Lookup_Tables',
    provide_context=False,
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",    
    table = "dim_location",
    sql_init_command = SqlQueries.location_table_create,
    copy_options="IGNOREHEADER 0 FORMAT AS JSON 'auto'",
    s3_path = "s3://lzalewsk-capstone/lookup_data/_i94cntyl.json",
    region="us-west-2"
)


lt_port_to_redshift = LoadLookupToRedshiftOperator(
    task_id='Load_Port_Lookup_Tables',
    provide_context=False,
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",    
    table = "dim_port",
    sql_init_command = SqlQueries.port_table_create,
    copy_options="IGNOREHEADER 0 FORMAT AS JSON 'auto'",
    s3_path = "s3://lzalewsk-capstone/lookup_data/_i94prtl.json",
    region="us-west-2"
)


lt_address_to_redshift = LoadLookupToRedshiftOperator(
    task_id='Load_Address_Lookup_Tables',
    provide_context=False,
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",    
    table = "dim_address",
    sql_init_command = SqlQueries.address_table_create,
    copy_options="IGNOREHEADER 0 FORMAT AS JSON 'auto'",
    s3_path = "s3://lzalewsk-capstone/lookup_data/_i94addrl.json",
    region="us-west-2"
)

lt_visa_to_redshift = LoadLookupToRedshiftOperator(
    task_id='Load_Visa_Lookup_Tables',
    provide_context=False,
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",    
    table = "dim_visa",
    sql_init_command = SqlQueries.visa_table_create,
    copy_options="IGNOREHEADER 0 FORMAT AS JSON 'auto'",
    s3_path = "s3://lzalewsk-capstone/lookup_data/_i94visa.json",
    region="us-west-2"
)

create_tables_operator = PostgresOperator(
    task_id = 'Create_Redshift_Tables',
    dag = dag,    
    postgres_conn_id= 'redshift',
    sql = SqlQueries.create_all_talbes,
)

run_quality_checks = DataQualityOperator(
    task_id='Data_quality_check_COUNT',
    dag=dag,
    redshift_conn_id="redshift",
    tables=['dim_mode','dim_location','dim_port','dim_address','dim_visa']
)


run_quality_check_dim_port = DataQualityOperator(
    task_id='Data_quality_check_dim_port',
    dag=dag,
    redshift_conn_id="redshift",
    test_sql = "select lon,lat from dim_port where port_code = 'HOM'",
    test_result = (-151.477,59.6456,)
)


run_quality_check_dim_address = DataQualityOperator(
    task_id='Data_quality_check_dim_address',
    dag=dag,
    redshift_conn_id="redshift",
    test_sql = "select address_name from dim_address where address_code = 'FL'",
    test_result = ('FLORIDA',)
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

# DAG definition

lookup_tables_tasks = [
    lt_mode_to_redshift,
    lt_location_to_redshift,
    lt_port_to_redshift,
    lt_address_to_redshift,
    lt_visa_to_redshift
]

quality_checks_tasks = [
    run_quality_check_dim_port,
    run_quality_check_dim_address,
    run_quality_checks
]

start_operator >> drop_db_schema >> lookup_tables_tasks >> create_tables_operator >> quality_checks_tasks >> end_operator