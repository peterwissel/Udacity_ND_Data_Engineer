from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    template_fields = ("s3_key",)

    # Copy data from S3 to Redshift in JSON format
    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        JSON '{}'
    """

    @apply_defaults
    def __init__(self,
                 # Operators params (with defaults)
                 aws_credentials_id="",
                 redshift_conn_id="",
                 s3_bucket="",
                 s3_key="",
                 operation_mode="",
                 destination_table="",
                 json_path="",
                 create_staging_table="",
                 *args, **kwargs):
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)

        # Mapped params
        self.aws_credentials_id = aws_credentials_id
        self.redshift_conn_id = redshift_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.operation_mode = operation_mode
        self.destination_table = destination_table
        self.json_path = json_path
        self.create_staging_table = create_staging_table

    def execute(self, context):
        self.log.info("StageToRedshiftOperator starts execution")

        # Log some information
        execution_date = context["execution_date"]
        self.log.info("Info about current execution date:")
        self.log.info(execution_date.year)
        self.log.info(execution_date.month)

        # Connection to AWS and Redshift
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info("Connection to AWS and Redshift has been made.")

        # Prepare staging table
        self.log.info("Create staging table if not exists")
        redshift.run("{}".format(self.create_staging_table))

        """"
        # Prepare an empty or clean data basis
        # It's much much faster to use the function TRUNCATE TABLE instead of DELETE to prepare a clean data basis
        """
        if self.operation_mode == 'truncate_load':
            self.log.info("Copy data works in TRUNCATE_LOAD mode. Truncate table data from destination Redshift "
                          "table ({})".format(self.destination_table))
            redshift.run("TRUNCATE TABLE {};".format(self.destination_table))
        elif self.operation_mode == 'append_only':
            self.log.info("Copy data works in APPEND_ONLY mode.")
        else:
            raise ValueError(f"staging_operation_mode = '{self.operation_mode}'; Variable was not set "
                             f"correctly. Use 'truncate_load' or 'append_only'.")

        self.log.info("Copying data from S3 to Redshift")

        # render key with formatted values from given parameters
        rendered_key = self.s3_key.format(**context)
        self.log.info(f"rendered S3-key: {rendered_key}")

        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.destination_table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.json_path
        )
        redshift.run(formatted_sql)
        self.log.info('StageToRedshiftOperator finished execution')
