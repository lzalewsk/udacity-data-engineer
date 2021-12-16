from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):
    """
    Airflow Operator loads Dimension tables from staging table using the queries from SqlQueries Helper Class
    """
    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 redshift_conn_id = "",
                 table = "",
                 sql = "",                 
                 remove_old_data = False,
                 *args, **kwargs):
        """
        Operator Class Init params:
        :param redshift_conn_id:        connection id used by Airflow to connect to Redshift
        :param table:                   name of target Dimension table
        :param sql:                     SQL query command to get Dimension data
        :param remove_old_data:<bool>   Default: False; If True - removes old data from table
        
        """
        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql = sql
        self.remove_old_data = remove_old_data

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
    
        if self.remove_old_data:
            self.log.info(f'Deleting data from {self.table} dimension table.')
            redshift.run(f'DELETE FROM {self.table};')
            
        self.log.info(f'Loading data into {self.table} dimension table.')
        formatted_sql = f"""
            INSERT INTO {self.table}
            {self.sql};
        """
        redshift.run(formatted_sql)
        self.log.info("DONE.")