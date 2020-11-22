from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DataQualityOperator(BaseOperator):
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 # Operators params (with defaults)
                 redshift_conn_id="",
                 dq_checks="",
                 *args, **kwargs):
        super(DataQualityOperator, self).__init__(*args, **kwargs)
        # Mapped params
        self.redshift_conn_id = redshift_conn_id
        self.dq_checks = dq_checks

    def execute(self, context):
        self.log.info('Executing DataQuality checks yet')

        # variable declaration
        error_count = 0
        failing_tests = []

        # Connection to Redshift
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info("Connection to Redshift has been made.")

        # Loop thru all dq_checks and execute them
        for check in self.dq_checks:
            check_name = check.get('check_name')
            sql = check.get('check_sql')
            check_expected_result = check.get('check_expected_result')

            # get amount of records
            records = redshift.get_records(sql)[0]
            self.log.info(check_name)
            self.log.info(f"Expected result: {check_expected_result}; current_result: {records[0]}")

            # check if current records fulfill the expected records due to the comparison operator
            if records[0] != check_expected_result:
                error_count += 1
                failing_tests.append(sql)

        # print out some error information and raise a ValueError
        if error_count > 0:
            self.log.error("DQ check failed")
            self.log.error(failing_tests)
            raise ValueError(f"Data quality check failures: {error_count}")

        self.log.info('Execution of DataQuality checks finished')
