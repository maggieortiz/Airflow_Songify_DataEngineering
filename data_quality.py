from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
#
#-- OPERATOR checks that there is data in users, artists, songs, songplays, & time table 
#
class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 #aws_credentials_id = "",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        #self.aws_credentials_id = aws_credentials_id
        
    def execute(self, context):
        table_list = ["artists", "songplays", "users", "songs", "time"]
        redshift_hook = PostgresHook(self.redshift_conn_id)
        i = 0
        while i < len(table_list): 
            records = redshift_hook.get_records(f"SELECT COUNT(*) FROM {table_list[i]}")
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f"Data quality check failed. {table_list[i]} returned no results")
                num_records = records[0][0]
            if num_records < 1:
                raise ValueError(f"Data quality check failed. {table_list[i]} contained 0 rows")
            logging.info(f"Data quality on table {table_list[i]} check passed with {records[0][0]} records")
            i = i + 1 
        
        self.log.info('DataQualityOperator completed and records in each table displayed')