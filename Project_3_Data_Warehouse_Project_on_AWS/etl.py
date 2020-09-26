import configparser
import datetime

import psycopg2
import boto3
from sql_queries import create_schema_and_search_path, insert_table_queries, copy_table_queries, \
    clean_up_after_processing


# Procedure to inform the user about the execution time
def time_tracker(method_name, execution_start, execution_end):
    execution_duration = execution_end - execution_start
    print("--------------------------------------" +"\n"
          "Method: " + method_name + "\n"
          "Total : Start: " + str(execution_start.strftime('%Y-%m-%d %H:%M:%S')) + "\n"
          "Total : End: " + str(execution_end.strftime('%Y-%m-%d %H:%M:%S')) + "\n"
          "Total : Duration: " + str(execution_duration) + "\n"
          "--------------------------------------"
          )


# Check if data to load resides in S3
def check_if_data_resides_in_S3(config, bucket_name, bucket_key):
    # for "time_tracker"
    execution_start = datetime.datetime.now()

    # create variables to initialize S3 client
    config.read_file(open('dwh.cfg'))

    s3_region_name = config.get("CLUSTER-DETAILS", "REGION_NAME")
    s3_key = config.get('AWS', 'KEY')
    s3_secret = config.get('AWS', 'SECRET')

    # instantiate S3 client
    s3 = boto3.resource('s3',
                        region_name=s3_region_name,
                        aws_access_key_id=s3_key,
                        aws_secret_access_key=s3_secret
                        )

    bucket_to_analyze = s3.Bucket(bucket_name)

    for obj in bucket_to_analyze.objects.filter(Prefix=bucket_key):
        print(obj)

    # for "time_tracker"
    execution_end = datetime.datetime.now()
    time_tracker("check_if_data_resides_in_S3", execution_start, execution_end)


# Create schema and switch search path to "Project3"
def prepare_schema_and_search_path(cur, conn):
    # for "time_tracker"
    execution_start = datetime.datetime.now()

    for query in create_schema_and_search_path:
        cur.execute(query)
        conn.commit()

    # for "time_tracker"
    execution_end = datetime.datetime.now()
    time_tracker("prepare_schema_and_search_path", execution_start, execution_end)


# get Content from S3 and copy it into Redshift staging table
def load_staging_tables(cur, conn):
    # for "time_tracker"
    execution_start = datetime.datetime.now()

    i = 1
    for query in copy_table_queries:
        print("LOAD Staging Table " + str(i) + " - Start")
        cur.execute(query)
        conn.commit()
        print("LOAD Staging Table " + str(i) + " - End")
        i += 1

    # for "time_tracker"
    execution_end = datetime.datetime.now()
    time_tracker("load_staging_tables", execution_start, execution_end)


# Transform content from staging tables into Star- or Dimension tables
def insert_tables(cur, conn):
    # for "time_tracker"
    execution_start = datetime.datetime.now()

    i = 1
    for query in insert_table_queries:
        print("INSERT Table " + str(i) + " - Start")
        cur.execute(query)
        conn.commit()
        print("INSERT Table " + str(i) + " - End")
        i += 1

    # for "time_tracker"
    execution_end = datetime.datetime.now()
    time_tracker("insert_tables", execution_start, execution_end)


# Drop the staging tables
def clean_up_after_processing_step(cur, conn):
    # for "time_tracker"
    execution_start = datetime.datetime.now()

    i = 1
    for query in clean_up_after_processing:
        print("CLEANUP AFTER PROCESSING - Start")
        cur.execute(query)
        conn.commit()
        print("CLEANUP AFTER PROCESSING - End")
        i += 1

    # for "time_tracker"
    execution_end = datetime.datetime.now()
    time_tracker("clean_up_after_processing_step", execution_start, execution_end)


def main():
    # for "time_tracker"
    total_execution_start = datetime.datetime.now()

    # Initialize environment and read configuration file
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    print("Initialized environment and read configuration file")

    # Establish DB-Connection to AWS Redshift
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    print("Established DB-Connection to AWS Redshift")

    # Prepare schema for work in Project 3
    prepare_schema_and_search_path(cur, conn)
    print("Prepared schema for work in Project 3")

    # Check if data to load resides in S3 --> only use for debugging purposes

    # Is song_data available to load in S3?
    bucket_name = 'udacity-dend'
    # 'song_data' bucket key value should never be loaded, because of it's amount of files
    # bucket_key = 'song_data'
    bucket_key = 'song_data/A/A/A/'
    check_if_data_resides_in_S3(config, bucket_name, bucket_key)

    # Is log_data available to load in S3?
    bucket_name = 'udacity-dend'
    bucket_key = 'log_data'
    check_if_data_resides_in_S3(config, bucket_name, bucket_key)

    # Load content from S3 to Redshift staging tables
    load_staging_tables(cur, conn)

    # After the data loading of S3 is completed, the further data loading into the Star Schema can begin
    insert_tables(cur, conn)

    # clean up staging tables and close existing connection
    clean_up_after_processing_step(cur, conn)
    conn.close()
    print("DB-Connection to AWS Redshift closed")
    print("Final Step: Execute the following commands in DB client: \n"
          "VACCUUM: Re-sorts rows and reclaims space in either a specified table or all tables in the current "
          "database.\n"
          "ANALYZE: Updates table statistics for use by the query planner.")

    # for "time_tracker"
    total_execution_end = datetime.datetime.now()
    time_tracker("ETL.PY", total_execution_start, total_execution_end)


if __name__ == "__main__":
    main()
