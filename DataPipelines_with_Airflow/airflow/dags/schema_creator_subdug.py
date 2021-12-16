from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from helpers import SqlQueries

def sparkify_schema_creator_dag(
        parent_dag_name,
        task_id,
        redshift_conn_id,
        drop_db_tables_if_exists = False,
        *args, **kwargs):
    """
    SubDag for DB schema creation for Fact and Dimension tables.
    In case of defined table relation (omited in this project).
    table creation order is important.
    """
    
    dag = DAG(
        f"{parent_dag_name}.{task_id}",
        **kwargs
    )
  
    # Create Dimension Tables
    create_user_table = PostgresOperator(
        task_id=f"create_user_table",
        dag=dag,
        postgres_conn_id=redshift_conn_id,
        sql=SqlQueries.user_table_create
    )

    create_artist_table = PostgresOperator(
        task_id=f"create_artist_table",
        dag=dag,
        postgres_conn_id=redshift_conn_id,
        sql=SqlQueries.artist_table_create
    )
    
    create_time_table = PostgresOperator(
        task_id=f"create_time_table",
        dag=dag,
        postgres_conn_id=redshift_conn_id,
        sql=SqlQueries.time_table_create
    )
    
    create_song_table = PostgresOperator(
        task_id=f"create_song_table",
        dag=dag,
        postgres_conn_id=redshift_conn_id,
        sql=SqlQueries.song_table_create
    )    

    # Create facts Table
    create_songplay_able = PostgresOperator(
        task_id=f"create_songplay_table",
        dag=dag,
        postgres_conn_id=redshift_conn_id,
        sql=SqlQueries.songplay_table_create
    )       
    
    
    # DAG definition
    
    if drop_db_tables_if_exists:
        # Drop DB schema task
        drop_db_schema = PostgresOperator(
            task_id=f"drop_db_schema",
            dag=dag,
            postgres_conn_id=redshift_conn_id,
            sql=SqlQueries.db_schema_drop
        )          
        drop_db_schema >> [create_user_table, create_artist_table, create_time_table]
        
    [create_user_table, create_artist_table, create_time_table] >> create_song_table
    create_song_table >> create_songplay_able

    return dag
