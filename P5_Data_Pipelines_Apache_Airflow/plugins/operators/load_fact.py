from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers.sql_queries import SqlQueries


class LoadFactOperator(BaseOperator):
    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 # Operators params (with defaults)
                 redshift_conn_id="",
                 operation_mode="",
                 fact_table_name="",
                 fact_table_sql_create="",
                 fact_table_sql_insert="",
                 fact_table_sql_truncate="",
                 *args, **kwargs):
        super(LoadFactOperator, self).__init__(*args, **kwargs)
        # Mapped params
        self.redshift_conn_id = redshift_conn_id
        self.operation_mode = operation_mode
        self.fact_table_name = fact_table_name
        self.fact_table_sql_create = fact_table_sql_create
        self.fact_table_sql_insert = fact_table_sql_insert
        self.fact_table_sql_truncate = fact_table_sql_truncate

    def execute(self, context):
        self.log.info(f"LoadFactOperator starts execution for table '{self.fact_table_name}'")

        # Connection to Redshift
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info("Connection to Redshift has been made.")

        # Create fact table
        self.log.info(f"Create fact table '{self.fact_table_name}' if not exists")
        redshift.run(f"{self.fact_table_sql_create}")
        self.log.info(f"Fact table '{self.fact_table_name}' has been created")

        # LOG information
        self.log.info(f"Inserting entries to fact table '{self.fact_table_name}'")

        # Check for the STATS output to see in log file how many rows have been inserted
        entries_before = redshift.get_first(f"SELECT COUNT(1) FROM {self.fact_table_name};")

        # Insert data into fact table. Operation_mode should be in 'append_only' mode
        if self.operation_mode == "append_only":
            self.log.info(f"Data for fact table '{self.fact_table_name}' works in "
                          f"'{self.operation_mode}' mode.")

            redshift.run(f"{self.fact_table_sql_insert}")
            self.log.info(f"Data for fact table '{self.fact_table_name}' has been inserted.")

        elif self.operation_mode == "truncate_load":
            self.log.info(f"Data for fact table '{self.fact_table_name}' works in "
                          f"'{self.operation_mode}' mode.")

            redshift.run(f"{self.fact_table_sql_truncate}")
            redshift.run(f"{self.fact_table_sql_insert}")
            self.log.info(f"Adding data to fact table '{self.fact_table_name}' should be in "
                          f"operation_mode = 'append_only'. This mode truncates table first before it inserts new "
                          f"entries!")
        else:
            raise ValueError(f"Please configure operation_mode == (\"truncate_load\" | \"append_only\").")

        # Check for the STATS output to see in log file how many rows have been inserted
        entries_after = redshift.get_first(f"SELECT COUNT(1) FROM {self.fact_table_name};")
        entries_inserted = entries_after[0] - entries_before[0]

        self.log.info(f"STATS: Before insert: {entries_before[0]}; After  insert: {entries_after[0]}; "
                      f"Diff: {entries_inserted}"
                      )
