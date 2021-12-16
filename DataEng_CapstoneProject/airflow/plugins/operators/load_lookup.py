from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadLookupToRedshiftOperator(BaseOperator):
    """
    Airflow Operator loads lookup tables from S3 bucket using the queries from SqlQueries Helper Class
    """
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 aws_credentials_id="",
                 table = "",
                 sql_init_command = "",
                 s3_path = "",
                 region= "us-west-2",
                 data_format = "",
                 copy_options = "",
                 *args, **kwargs):
        """
        Operator Class Init params:
        :param redshift_conn_id:    connection id used by Airflow to connect to Redshift
        :param aws_credentials_id:  aws credential id used to get IAM access key to get data from S3
        :param table:               name of target staging table to copy data from S3
        :param sql_init_command:    initial SQL command for drop and create target table
        :param s3_path:             data location s3_path
        :param copy_options:        additional SQL options for COPY operation
        :param region:              AWS region
        
        """
        super(LoadLookupToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.sql_init_command = sql_init_command
        self.s3_path = s3_path
        self.copy_options = copy_options
        self.region = region
        self.execution_date = kwargs.get('execution_date')

    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info(f"Drop and creates {self.table} stage table in Redshift")
        redshift.run(self.sql_init_command)
                
        self.log.info("Copying data from S3 to Redshift")
        formatted_sql = f"""
            COPY {self.table}
            FROM '{self.s3_path}'
            ACCESS_KEY_ID '{credentials.access_key}'
            SECRET_ACCESS_KEY '{credentials.secret_key}'          
            {self.copy_options}
            --STATUPDATE ON
            --REGION '{self.region}'
            ;
            """
        redshift.run(formatted_sql)        
