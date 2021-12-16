from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):
    """
    Airflow Operator for Data Quality check
    """
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 tables = [],
                 test_sql=None,
                 test_result=None,                 
                 *args, **kwargs):
        """
        Operator Class Init params:
        :param redshift_conn_id:    connection id used by Airflow to connect to Redshift
        :param tables:              array of tables to check if there are any data      
        :param test_sql:            Test SQL query
        :param test_result:         Expected Test result
        
        """
        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tables = tables
        self.test_sql = test_sql
        self.test_result = test_result

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        # Check all tables from tables array
        for table in self.tables:
            records = redshift.get_records(f"SELECT COUNT(*) FROM {table}")
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f"Fail: No results for {table}")
            num_records = records[0][0]
            if num_records < 1:
                raise ValueError(f"Fail: 0 rows in {table}")
            self.log.info(f"Success: {table} has {records[0][0]} records")

        if self.test_sql:
            output = redshift.get_first(self.test_sql)
            if self.test_result != output:
                raise ValueError(f"Fail: {output} != {self.test_result}")
                