import logging
from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators import (StageToRedshiftOperator,
                               LoadFactOperator,
                               LoadDimensionOperator,
                               DataQualityOperator
                               )

from helpers import SqlQueries


# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

# Print out some information to log file
def execution_logging(*args, **kwargs):
    execution_step = kwargs["params"]["execution_step"]
    logging.info(f"{execution_step} execution step is executed.")


default_args = {
    'owner': 'Peter Wissel',
    'depends_on_past': False,

    'start_date': datetime(2018, 11, 1),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False,
    # If this default value is not overridden the data would only be appended to the stage tables.
    'operation_mode': 'append_only'
}

# current DAG with it's configuration
dag = DAG('udac_project_data_pipeline_dag_final',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='@daily',
          max_active_runs=1,
          catchup=False
          )

# Start of execution
start_operator = PythonOperator(
    task_id='Begin_execution',
    dag=dag,
    python_callable=execution_logging,
    provide_context=True,
    params={
        'execution_step': 'BEGIN',
    }
)

# Load STAGING tables
stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    provide_context=True,

    # define credentials
    aws_credentials_id="aws_credentials",
    redshift_conn_id="redshift",

    # S3 source location
    s3_bucket="udacity-dend",

    #
    # The following line is requested in paragraph "Stage Operator". I commented it out due to the fact that there
    # is no source data on the provided S3 storage. I activated instead the line with a given year and month (2018/11)
    # s3_key="log_data/{execution_date.year}/{execution_date.month}",
    # s3_key="log_data/2018/11",

    # ### There values are for development / debugging purposes only ###
    s3_key="log_data",
    # s3_key="log_data/2018/11/2018-11-01-events.json",
    # s3_key="log_data/{execution_date.year}/{execution_date.month}/{ds}-events.json",

    # Override operation_mode
    # Decide between two modes: 'truncate_load' or 'append_only'. Recommended mode: 'append_only'
    operation_mode="append_only",

    # Redshift Destination table
    destination_table="staging_events",
    # json_path="auto",
    json_path="s3://udacity-dend/log_json_path.json",

    # Transfer all necessary information to the StageToRedshiftOperator for further processing
    create_staging_table=SqlQueries.staging_events_table_create,
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    provide_context=True,

    # define credentials
    aws_credentials_id="aws_credentials",
    redshift_conn_id="redshift",

    # S3 source location
    s3_bucket="udacity-dend",
    s3_key="song_data",
    # s3_key="song_data/A/A",
    # s3_key="song_data/A/A/A",
    # s3_key="song_data/A/A/A/TRAAAAK128F9318786.json",

    # Override operation_mode
    # Decide between two modes: 'truncate_load' or 'append_only'. Recommended mode: 'append_only'
    operation_mode="append_only",

    # Redshift Destination table
    destination_table="staging_songs",
    json_path="auto",

    # Transfer all necessary information to the StageToRedshiftOperator for further processing
    create_staging_table=SqlQueries.staging_songs_table_create,
)

# Load FACT tables
load_f_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    provide_context=True,

    # define connection
    redshift_conn_id="redshift",

    # Override operation_mode
    # Decide between two modes: 'truncate_load' or 'append_only'. Recommended mode: 'append_only'
    operation_mode="append_only",

    # Transfer all necessary information to the LoadDimensionOperator for further processing
    fact_table_name="f_songplays",
    fact_table_sql_create=SqlQueries.songplay_table_create,
    fact_table_sql_insert=SqlQueries.songplay_table_insert,
    fact_table_sql_truncate=SqlQueries.songplay_table_truncate
)

# Load DIMENSION tables
load_d_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    provide_context=True,

    # define connection
    redshift_conn_id="redshift",

    # Override operation_mode
    # Decide between two modes: 'truncate_load' or 'append_only'. Recommended mode: 'truncate_load'
    operation_mode="truncate_load",

    # Transfer all necessary information to the LoadDimensionOperator for further processing
    dim_table_name="d_users",
    dim_table_sql_create=SqlQueries.user_table_create,
    dim_table_sql_insert=SqlQueries.user_table_insert,
    dim_table_sql_truncate=SqlQueries.user_table_truncate
)

load_d_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    provide_context=True,

    # define connection
    redshift_conn_id="redshift",

    # Override operation_mode
    # Decide between two modes: 'truncate_load' or 'append_only'. Recommended mode: 'truncate_load'
    operation_mode="truncate_load",

    # Transfer all necessary information to the LoadDimensionOperator for further processing
    dim_table_name="d_songs",
    dim_table_sql_create=SqlQueries.song_table_create,
    dim_table_sql_insert=SqlQueries.song_table_insert,
    dim_table_sql_truncate=SqlQueries.song_table_truncate
)

load_d_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    provide_context=True,

    # define connection
    redshift_conn_id="redshift",

    # Override operation_mode
    # Decide between two modes: 'truncate_load' or 'append_only'. Recommended mode: 'truncate_load'
    operation_mode="truncate_load",

    # Transfer all necessary information to the LoadDimensionOperator for further processing
    dim_table_name="d_artists",
    dim_table_sql_create=SqlQueries.artist_table_create,
    dim_table_sql_insert=SqlQueries.artist_table_insert,
    dim_table_sql_truncate=SqlQueries.artist_table_truncate
)

load_d_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    provide_context=True,

    # define connection
    redshift_conn_id="redshift",

    # Override operation_mode
    # Decide between two modes: 'truncate_load' or 'append_only'. Recommended mode: 'truncate_load'
    operation_mode="truncate_load",

    # Transfer all necessary information to the LoadDimensionOperator for further processing
    dim_table_name="d_time",
    dim_table_sql_create=SqlQueries.time_table_create,
    dim_table_sql_insert=SqlQueries.time_table_insert,
    dim_table_sql_truncate=SqlQueries.time_table_truncate
)

#  Data quality checks. You're free to define more or less complicated tests
run_quality_checks = DataQualityOperator(
    task_id='run_data_quality_checks',
    dag=dag,
    redshift_conn_id='redshift',
    dq_checks=[
        {
            'check_name': "Check table 'f_songplays' on column 'level' for NULL values",
            'check_sql': "select count(1) from f_songplays where level is null;",
            'check_expected_result': 0
        },
        {
            'check_name': "Check table 'f_songplays' on column 'artist_id' for NULL values",
            'check_sql': "select count(1) from f_songplays where artist_id is null;",
            'check_expected_result': 6500
        },
        {
            'check_name': "Check table 'users' on column 'level' for NULL values",
            'check_sql': "select count(1) from d_users where level is null;",
            'check_expected_result': 0
        },
        {
            'check_name': "Check table 'f_songplays' and column 'level' for unexpected values ('paid', 'free')",
            'check_sql': "select count(1) from (select distinct level from f_songplays where level not in "
                         "('paid', 'free'));",
            'check_expected_result': 0
        }
    ]
)

# End of execution
end_operator = PythonOperator(
    task_id='Stop_execution',
    dag=dag,
    python_callable=execution_logging,
    provide_context=True,
    params={
        'execution_step': 'END',
    }
)

# # Configure dependencies
## The commands below describes the more detailed way

# start_operator >> stage_events_to_redshift
# start_operator >> stage_songs_to_redshift
#
# stage_events_to_redshift >> load_f_songplays_table
# stage_songs_to_redshift >> load_f_songplays_table
#
# load_f_songplays_table >> load_d_song_dimension_table
# load_f_songplays_table >> load_d_user_dimension_table
# load_f_songplays_table >> load_d_artist_dimension_table
# load_f_songplays_table >> load_d_time_dimension_table
#
# load_d_song_dimension_table >> run_quality_checks
# load_d_user_dimension_table >> run_quality_checks
# load_d_artist_dimension_table >> run_quality_checks
# load_d_time_dimension_table >> run_quality_checks
#
# run_quality_checks >> end_operator

## This way is more readable
start_operator \
>> [stage_events_to_redshift, stage_songs_to_redshift] \
>> load_f_songplays_table \
>> [load_d_song_dimension_table, load_d_user_dimension_table, load_d_artist_dimension_table,
    load_d_time_dimension_table] \
>> run_quality_checks \
>> end_operator

