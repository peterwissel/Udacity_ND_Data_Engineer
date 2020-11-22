from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadDimensionOperator(BaseOperator):
    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 # Operators params (with defaults)
                 redshift_conn_id="",
                 operation_mode="",
                 dim_table_name="",
                 dim_table_sql_create="",
                 dim_table_sql_insert="",
                 dim_table_sql_truncate="",
                 *args, **kwargs):
        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        # Mapped params
        self.redshift_conn_id = redshift_conn_id
        self.operation_mode = operation_mode
        self.dim_table_name = dim_table_name
        self.dim_table_sql_create = dim_table_sql_create
        self.dim_table_sql_insert = dim_table_sql_insert
        self.dim_table_sql_truncate = dim_table_sql_truncate

    def execute(self, context):
        self.log.info(f"LoadDimensionOperator starts execution for table '{self.dim_table_name}'")

        # Connection to Redshift
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info("Connection to Redshift has been made.")

        # Create dim table
        self.log.info(f"Create dim table '{self.dim_table_name}' if not exists")
        redshift.run(f"{self.dim_table_sql_create}")
        self.log.info(f"Dimension table '{self.dim_table_name}' has been created")

        # LOG information
        self.log.info(f"Inserting entries to dim table '{self.dim_table_name}'")

        # Check for the STATS output to see in log file how many rows have been inserted
        entries_before = redshift.get_first(f"SELECT COUNT(1) FROM {self.dim_table_name};")

        # Insert data into dim table. Operation_mode should be in 'truncate_load' mode
        if self.operation_mode == "truncate_load":
            self.log.info(f"Data for dim table '{self.dim_table_name}' works in "
                          f"'{self.operation_mode}' mode.")

            redshift.run(f"{self.dim_table_sql_truncate}")
            redshift.run(f"{self.dim_table_sql_insert}")

            self.log.info(f"Data for dim table '{self.dim_table_name}' has been inserted.")

        elif self.operation_mode == "append_only":
            self.log.info(f"Data for dim table '{self.dim_table_name}' works in "
                          f"'{self.operation_mode}' mode.")

            redshift.run(f"{self.dim_table_sql_insert}")
            self.log.info(f"Adding data to dimension table '{self.dim_table_name}' should in "
                          f"operation_mode = 'truncate_load'. This mode inserts ONLY new entries!")
        else:
            raise ValueError(f"Please configure operation_mode == (\"truncate_load\" | \"append_only\").")

        # Check for the STATS output to see in log file how many rows have been inserted
        entries_after = redshift.get_first(f"SELECT COUNT(1) FROM {self.dim_table_name};")
        entries_inserted = entries_after[0] - entries_before[0]

        self.log.info(f"STATS: Before insert: {entries_before[0]}; After  insert: {entries_after[0]}; "
                      f"Diff: {entries_inserted}"
                      )
