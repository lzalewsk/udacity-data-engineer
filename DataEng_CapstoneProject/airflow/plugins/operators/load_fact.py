from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):
    """
    Airflow Operator loads Fact tables from staging table using the queries from SqlQueries Helper Class
    """
    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 redshift_conn_id = "",
                 table = "",
                 sql = "",
                 *args, **kwargs):
        """
        Operator Class Init params:
        :param redshift_conn_id:    connection id used by Airflow to connect to Redshift
        :param table:               name of target fact table
        :param sql:                 SQL query command to get fact data
        
        """
        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql = sql

    def execute(self, context):        
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        self.log.info(f'Loading data into {self.table} fact table.')
        formatted_sql = f"""
            INSERT INTO {self.table}
            {self.sql};
        """
        redshift.run(formatted_sql)
        self.log.info("DONE.")