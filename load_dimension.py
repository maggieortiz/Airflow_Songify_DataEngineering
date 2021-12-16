from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
#
# -- OPERATOR INSERTS DATA INTO DIMENSION TALBES (artists, users, songs, time) from stageing_events & staging_songs
#
class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 redshift_conn_id = "",
                 aws_credential_id = "",
                 sql ="",
                 target_table = "",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.aws_credential_id = aws_credential_id
        self.redshift_conn_id = redshift_conn_id
        self.sql = sql
        self.target_table = target_table
        
    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift_hook = PostgresHook(self.redshift_conn_id)
        sql_stm = f"INSERT INTO {self.target_table} {self.sql}"
        redshift_hook.run(sql_stm)
        self.log.info('LoadDimensionOperator load dimension table {self.table}')
