from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook
#
#--- OPERATOR CONNECTS TO REDSHIFT AND COPYS INFORMATION FROM log_data & song_data TO staging_event & stagin_song tables 
#

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    #template_fields = ("s3_key",)
    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        REGION '{}'
        JSON '{}'
        TIMEFORMAT as 'epochmillisecs'
        TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL
    """
    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 aws_credentials_id = "",
                 s3bucket = "",
                 s3path = "",
                 table = "",
                 region = "",
                 json = "",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.s3bucket = s3bucket
        self.s3path = s3path
        self.table = table 
        self.region = region
        self.json = json

    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        fromPath = f"s3://{self.s3bucket}/{self.s3path}"
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            fromPath,
            credentials.access_key,
            credentials.secret_key,
            self.region,
            self.json
        )
        redshift.run(formatted_sql)
        
        self.log.info(f'StageToRedshiftOperator created {self.table}')





