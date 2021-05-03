from datetime import datetime, timedelta
from os import path, mkdir
from pathlib import Path
from pyspark.sql.functions import lit
from pyspark.sql.types import IntegerType, DateType, StringType, StructType, StructField
from pyspark.sql.window import Window
from pyspark.sql import SparkSession
import pyspark.sql.functions as sql_f
from typing import Dict
import shutil
import json
import configparser


def time_tracker(method_name, execution_start, execution_end):
    """
    Procedure to inform the user about the execution time and duration
    :param method_name: STRING - Name of the executed code to distinguish between different executions
    :param execution_start: DATETIME - datetime.datetime.now() --> Return the current local date and time
    :param execution_end: DATETIME - datetime.datetime.now() --> Return the current local date and time
    :return STRING - Listing of start and end time including duration
    """
    execution_duration = execution_end - execution_start
    print("")
    print("--------------------------------------")
    print("Method: {}".format(method_name))
    print("Total : Start: {}".format(str(execution_start.strftime('%Y-%m-%d %H:%M:%S'))))
    print("Total : End: {}".format(str(execution_end.strftime('%Y-%m-%d %H:%M:%S'))))
    print("Total : Duration: {}".format(str(execution_duration)))
    print("--------------------------------------")


def set_up_environment_variables():
    """
    Set up all environment variables in one Dict and return it
    :return Dict of all environment variables
    """
    # initialization
    config = configparser.ConfigParser()
    config.read_file(open('P8_Capstone_Project_ETL_Processing_Data.cfg'))

    # set variables for current function
    return_environment_variables: Dict[str, str] = {}

    # loop thru all SECTIONS and ITEMS from config file
    for each_section in config.sections():
        for (each_key, each_val) in config.items(each_section):
            return_environment_variables[f"{each_key}"] = f"{each_val}"

    # return all variables from config (cfg) file
    return return_environment_variables


def create_spark_session():
    """
    Get or Create Spark Session
    - load a special package:  saurfang:spark-sas7bdat --> This package reads data in SAS format

    :return SparkSession
    """
    # Increase spark default memory size due to very heavy memory consumpting dataframes
    max_memory = "5g"

    spark_session = SparkSession \
        .builder \
        .appName("etl pipeline for project 8 - I94 data") \
        .config("spark.jars.packages", "saurfang:spark-sas7bdat:3.0.0-s_2.12") \
        .config('spark.sql.repl.eagerEval.enabled', True) \
        .config("spark.executor.memory", max_memory) \
        .config("spark.driver.memory", max_memory) \
        .enableHiveSupport() \
        .getOrCreate()

    # setting the current LOG-Level
    spark_session.sparkContext.setLogLevel('ERROR')

    # return current session
    return spark_session


def convert_i94_sas_data_into_parquet_staging_files(spark_session, environment_variables):
    """
    Convert SAS data into parquet files as 1st staging step

    If data folder already exists read data instead of creating it again
    :param spark_session: use existing spark session
    :param environment_variables: get all available environment variables
    """
    # get environment variables
    i94_data_raw_sas = environment_variables.get("i94_data_raw_sas_path")
    i94_data_raw_parquet = environment_variables.get("i94_data_raw_parquet_path")

    # check if data already exists. If true --> work is already done!
    if folder_exists(i94_data_raw_parquet):
        print(f"SAS data is already converted. It is located in folder: {i94_data_raw_parquet}")

    # convert raw source data from SAS data format into parquet data format
    else:
        # The SAS files (e.g. i94_apr16_sub.sas7bdat) are partitioned by month. The for loop extracts each file and
        # stores it partitioned by month in parquet format.
        months_abbreviation = ["jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec"]

        for current_month in months_abbreviation:
            month_abbreviation = current_month

            filepath_i94 = f"{i94_data_raw_sas}/i94_{month_abbreviation}16_sub.sas7bdat"

            # load current month
            df_spark_i94 = spark_session \
                .read \
                .format('com.github.saurfang.sas.spark') \
                .load(filepath_i94)

            # write information to console
            print(f"{filepath_i94} loaded")

            # write data frame as parquet file (ca. 815 MB) and append all month to the same parquet result set
            df_spark_i94 \
                .repartition(int(1)) \
                .write \
                .mode(saveMode='append') \
                .partitionBy('i94mon') \
                .parquet(i94_data_raw_parquet, compression="gzip")

            # write information to console
            print(f"{i94_data_raw_parquet} written")

        # Read written data frame back into memory
        df_spark_i94 = spark_session.read.parquet(i94_data_raw_parquet)

        # some output to console
        print(f"{df_spark_i94.count()} entries inside data frame.")
        print("Data conversion finished")


def get_date_from_sas_date(sas_date):
    """
    Convert SAS date into a DateType value. If sas_date == 0 then choose the default value 1960-01-01.
    :param sas_date: Integer value which represents the SAS date.
    :return DateTime Object in ISO-Format
    """
    # Function (UDF) to convert a SAS date (Integer Format) into a DateType() format.
    sas_date_int = int(sas_date)
    if sas_date_int > 0:
        return datetime(1960, 1, 1) + timedelta(days=sas_date_int)
    else:
        return datetime(1900, 1, 1)


def folder_exists(folder_to_check_path):
    """
    Check if folder already exists in filesystem

    :param folder_to_check_path: folder to check if it exists
    :return True or False
    """

    # check if path in filesystem exists
    if path.exists(folder_to_check_path):
        # Folder exists
        return True
    else:
        # Folder does not exist. You have to create one if needed.
        return False


def folder_to_delete(folder_to_delete_path):
    """
    Delete a given folder from filesystem

    :param folder_to_delete_path: Folder to delete in filesystem
    """

    # delete folder, only if it exists
    if path.exists(folder_to_delete_path):
        shutil.rmtree(folder_to_delete_path)
        print("folder deleted: " + str(folder_to_delete_path))
    else:
        print(f"folder exists:  {str(path.exists(folder_to_delete_path))}. Nothing happened!")


def folder_to_create(folder_to_create_path):
    """
    Create a folder in filesystem if it does not exist

    : param folder_to_create_path: Path in filesystem to create
    """
    # create a folder if it not exists
    if path.exists(folder_to_create_path):
        print(f"folder already exists: {str(path.exists(folder_to_create_path))}. Nothing will happen!")
    else:
        Path(folder_to_create_path).mkdir(parents=True, exist_ok=True)
        print("folder created: " + str(folder_to_create_path))


def dataframe_to_create(spark_session, environment_variables, staging_tables_available, dataframe_to_create_variable):
    """
    Function is like a switch dispatcher. It calls functions which depends on the given parameters.

    :param spark_session: current spark session
    :param environment_variables: all available environment variables
    :param staging_tables_available: dict with already loaded dataframes
    :param dataframe_to_create_variable: name of the current function to call
    :return The function within the "switch - case" command
    """
    return {
        # switch - case
        'st_immigration_airports': lambda: staging_table_create_st_immigration_airports(spark_session,
                                                                                        environment_variables),
        'st_immigration_countries': lambda: staging_table_create_st_immigration_countries(spark_session,
                                                                                          environment_variables),
        'st_i94_immigrations': lambda: staging_table_create_st_i94_immigrations(spark_session, environment_variables,
                                                                                staging_tables_available),
        'st_date_arrivals': lambda: staging_table_create_st_date_arrivals(spark_session, environment_variables,
                                                                          staging_tables_available),
        'st_date_departures': lambda: staging_table_create_st_date_departures(spark_session, environment_variables,
                                                                              staging_tables_available),
        'st_state_destinations': lambda: staging_table_create_st_state_destinations(spark_session, environment_variables
                                                                                    , staging_tables_available),
        'd_immigration_countries': lambda: dimension_table_create_d_immigration_countries(spark_session,
                                                                                          environment_variables,
                                                                                          staging_tables_available),
        'd_immigration_airports': lambda: dimension_table_create_d_immigration_airports(spark_session,
                                                                                        environment_variables,
                                                                                        staging_tables_available),
        'd_date_arrivals': lambda: dimension_table_create_d_date_arrivals(spark_session, environment_variables,
                                                                          staging_tables_available),
        'd_date_departures': lambda: dimension_table_create_d_date_departures(spark_session, environment_variables,
                                                                              staging_tables_available),
        'd_state_destinations': lambda: dimension_table_create_d_state_destinations(spark_session,
                                                                                    environment_variables,
                                                                                    staging_tables_available),
        'f_i94_immigrations': lambda: fact_table_create_f_i94_immigrations(spark_session, environment_variables,
                                                                           staging_tables_available)
        # call dict.get(...) method to the get function to execute next command. Default value is NONE
    }.get(environment_variables.get(f"{dataframe_to_create_variable}_name"), lambda: None)()


def dataframe_to_load(spark_session, environment_variables, staging_tables_available, dataframe_to_load_name_variable):
    """
    Load the dataframe into memory from filesystem. If dataframe already exists, read it from filesystem. If not, create
    a new dataframe.

    :param spark_session: current spark session
    :param environment_variables: all available environment variables
    :param staging_tables_available: dict with already loaded dataframes
    :param dataframe_to_load_name_variable: name of the current dataframe to load
    :return loaded dataframe
    """
    # get environment variables based on the given parameter
    dataframe_to_load_name = environment_variables.get(f"{dataframe_to_load_name_variable}_name")
    dataframe_to_load_path = environment_variables.get(f"{dataframe_to_load_name_variable}_path")

    # load dataframe only if path exists
    if folder_exists(dataframe_to_load_path):
        print(f"Dataframe {dataframe_to_load_name} loaded.")
        return spark_session.read.parquet(dataframe_to_load_path)

    # if path does not exist, dataframe does not exist as well. Create and persist it!
    else:
        # create not existing dataframe
        dataframe_to_persist_data = dataframe_to_create(spark_session, environment_variables, staging_tables_available,
                                                        dataframe_to_load_name)
        print(f"Dataframe {dataframe_to_load_name} created.")

        # Write dataframe to disk to persist it for later use
        persist_data_frame(spark_session, environment_variables, dataframe_to_load_name, dataframe_to_persist_data)
        print(f"Dataframe {dataframe_to_load_name} persisted.")
        return dataframe_to_persist_data


def get_dataframe(spark_session, environment_variables, data_table_available, data_table_to_load_variable):
    """
    Deliver the caller a dataframe. If it is not already loaded in dict 'staging_tables_available' then load it from
    filesystem.

    :param spark_session: current spark session
    :param environment_variables: all available environment variables
    :param data_table_available: dict with all already loaded dataframes
    :param data_table_to_load_variable: name of the current dataframe to load
    :return loaded dataframe
    """

    # get configured environment variable. This could differ from parameter name
    staging_table_to_load_name = environment_variables.get(f"{data_table_to_load_variable}_name")

    # If dataframe is already available, get it from dict 'staging_tables_available'
    if data_table_available.get(staging_table_to_load_name):
        dataframe_loaded = data_table_available.get(staging_table_to_load_name)
        print(f"Dataframe {staging_table_to_load_name} loaded from already existing im memory dict "
              f"'staging_tables_available'.")
    else:
        # Dataframe does not exist? Load it!
        dataframe_loaded = dataframe_to_load(spark_session, environment_variables, data_table_available,
                                             staging_table_to_load_name)
        # put loaded dataframe into dict 'staging_tables_available' for later use if needed.
        data_table_available[staging_table_to_load_name] = dataframe_loaded
    return dataframe_loaded


def persist_data_frame(spark_session, environment_variables, dataframe_to_persist_name, dataframe_to_persist_data):
    """
    Persist a data frame to filesystem

    :param spark_session: current spark session
    :param environment_variables: all available environment variables
    :param dataframe_to_persist_name: name to get the current configured environment variable
    :param dataframe_to_persist_data: dataframe to persist
    """
    # configure environment variables to persist current data frame
    dataframe_to_persist_path = environment_variables.get(f"{dataframe_to_persist_name}_path")
    dataframe_to_persist_partition_by = environment_variables.get(f"{dataframe_to_persist_name}_partition_by").split(
        ',')

    # check if path of df to persist already exists. TRUE = delete it first
    if path.exists(dataframe_to_persist_path):
        # If data already exists, clean up and delete it first. This prevents you from getting write errors when the df
        # structure changes.
        folder_to_delete(dataframe_to_persist_path)

    # write data frame as parquet file
    dataframe_to_persist_data \
        .repartition(int(1)) \
        .write \
        .format("parquet") \
        .mode(saveMode='overwrite') \
        .partitionBy(dataframe_to_persist_partition_by) \
        .parquet(dataframe_to_persist_path, compression="gzip")

    # Output some information
    print(f"data persisted successfully to folder {dataframe_to_persist_path}")


def persist_data_frame_json(spark_session, environment_variables, dataframe_to_persist_name, dataframe_to_persist_data):
    """
    Persist a data frame to filesystem in JSON format

    :param spark_session: current spark session
    :param environment_variables: all available environment variables
    :param dataframe_to_persist_name: name to get the current configured environment variable
    :param dataframe_to_persist_data: dataframe to persist
    """

    # configure environment variables to persist current data frame

    dataframe_to_persist = environment_variables.get(f"{dataframe_to_persist_name}_name")
    dataframe_to_persist_file = dataframe_to_persist + ".json"
    dataframe_to_persist_path = environment_variables.get(f"{dataframe_to_persist_name}_path_json")
    dataframe_to_persist_full_path_json = path.join(dataframe_to_persist_path, dataframe_to_persist_file)

    # check if path of df to persist already exists. TRUE = create it first
    if not path.exists(dataframe_to_persist_path):
        folder_to_create(dataframe_to_persist_path)

    # write data frame as JSON file
    json_data_to_persist = dataframe_to_persist_data.toJSON().collect()
    with open(dataframe_to_persist_full_path_json, "w") as outfile:
        json.dump(json_data_to_persist, outfile, sort_keys=True, indent=4, ensure_ascii=False)

    # Output some information
    print(f"JSON data persisted successfully to file {dataframe_to_persist_full_path_json}")


def staging_table_create_st_i94_immigrations(spark_session, environment_variables, staging_tables_available):
    """
    convert SAS date into ISO format, select only needed columns, reduce duplicates and create staging table
    df_st_i94_immigrations

    :param spark_session: current spark session
    :param environment_variables: all available environment variables
    :param staging_tables_available: already loaded staging tables into dict
    :return: staging table `st_i94_immigrations` as data frame (df)
    """
    # read data from parquet files.
    table_to_load_variable = "st_immigration_airports"
    staging_table_to_load_name = environment_variables.get(f"{table_to_load_variable}_name")
    df_st_immigration_airports = get_dataframe(spark_session, environment_variables, staging_tables_available,
                                               staging_table_to_load_name)

    # SAS data
    table_to_load_variable = "i94_data_raw_parquet"
    staging_table_to_load_name = environment_variables.get(f"{table_to_load_variable}_name")
    df_st_i94_immigrations = get_dataframe(spark_session, environment_variables, staging_tables_available,
                                           staging_table_to_load_name)

    # register UDF function to calculate a DateType from given SAS date format
    get_date_fom_sas_date = sql_f.udf(lambda y: get_date_from_sas_date(y), DateType())
    spark_session.udf.register("get_date_fom_sas_date", get_date_fom_sas_date)

    # fill up null values
    df_st_i94_immigrations = df_st_i94_immigrations \
        .fillna(value=0.0, subset=['i94cit']) \
        .fillna(value='99', subset=['i94addr']) \
        .fillna(value=0.0, subset=['depdate']) \
        .fillna(value='99991231', subset=['dtadfile']) \
        .fillna(value='NA', subset=['matflag'])

    # add some new columns to staging table (df)
    df_st_i94_immigrations = df_st_i94_immigrations \
        .withColumn("st_i94_cit", sql_f.round("i94cit", 0).cast(IntegerType())) \
        .withColumn("st_i94_port", sql_f.col("i94port")) \
        .withColumn("st_i94_addr", sql_f.col("i94addr")) \
        .withColumn("st_i94_arrdate", sql_f.round("arrdate").cast(IntegerType())) \
        .withColumn("st_i94_arrdate_iso", get_date_fom_sas_date("arrdate")) \
        .withColumn("st_i94_depdate", sql_f.round("depdate").cast(IntegerType())) \
        .withColumn("st_i94_depdate_iso", get_date_fom_sas_date("depdate")) \
        .withColumn('st_i94_dtadfile', sql_f.to_date('dtadfile', 'yyyyMMdd')) \
        .withColumn("st_i94_matflag", sql_f.col("matflag")) \
        .withColumn("st_i94_count", sql_f.round("count", 0).cast(IntegerType())) \
        .withColumn("st_i94_year", sql_f.col("i94yr").cast(IntegerType())) \
        .withColumn("st_i94_month", sql_f.col("i94mon").cast(IntegerType()))

    # select only needed columns
    df_st_i94_immigrations = df_st_i94_immigrations \
        .select(
        "st_i94_cit",
        "st_i94_port",
        "st_i94_addr",
        "st_i94_arrdate",
        "st_i94_arrdate_iso",
        "st_i94_depdate",
        "st_i94_depdate_iso",
        "st_i94_dtadfile",
        "st_i94_matflag",
        "st_i94_count",
        "st_i94_year",
        "st_i94_month"
    )

    # Drop duplicate rows from dataframe and print out result of entries
    df_st_i94_immigrations = df_st_i94_immigrations.dropDuplicates()
    print(f"Content of table df_st_i94_immigrations : {df_st_i94_immigrations.count()}")

    # After duplicates are removed, a unique ID is created for each row.
    # The sql_f.row_number().over(w)) method gives each record a unique and increasing ID and starts with 1.
    w = Window().orderBy(lit('A'))

    df_st_i94_immigrations = df_st_i94_immigrations \
        .sort("st_i94_year", "st_i94_month", "st_i94_cit") \
        .withColumn("st_i94_id", sql_f.row_number().over(w))

    """
    Add the column `st_ia_airport_state_code --> st_i94_port_state_code` to staging table `st_i94_immigration` based on 
    staging table `st_immigration_airports`. This information is needed to connect the `us-cities-demographics.json` 
    file later on.
    """

    # add column `st_i94_port_state_code` to data frame st_i94_immigrations
    df_st_i94_immigrations = df_st_i94_immigrations \
        .join(df_st_immigration_airports,
              [df_st_i94_immigrations.st_i94_port == df_st_immigration_airports.st_ia_airport_code], 'left_outer') \
        .drop("st_ia_airport_code", "st_ia_airport_name") \
        .fillna(value='NA', subset=['st_ia_airport_state_code']) \
        .withColumnRenamed("st_ia_airport_state_code", "st_i94_port_state_code")

    """
    Clean date column "st_i94_depdate_iso": Valid entries are between 2016-01-01 and 2017-06-14. Pre- and descending 
    values will be set to null / default value (1900-01-01). 
    
    More detailed information is at line 1458 in file P8_Capstone_Project_Data_Preparation_Step_All.ipynb
    """
    # cleansing the date values in column `st_i94_depdate_iso`
    df_st_i94_immigrations = df_st_i94_immigrations \
        .withColumn("st_i94_depdate_iso",
                    sql_f.when(sql_f.col("st_i94_depdate_iso") < "2016-01-01", "1900-01-01")
                    .when(sql_f.col("st_i94_depdate_iso") > "2017-06-14", "1900-01-01")
                    .when(sql_f.col("st_i94_arrdate_iso") > sql_f.col("st_i94_depdate_iso"), "1900-01-01")
                    .otherwise(sql_f.col("st_i94_depdate_iso")).cast(DateType()))

    return df_st_i94_immigrations


def staging_table_create_st_immigration_countries(spark_session, environment_variables):
    """
    Create staging table st_immigration countries from file 'I94_SAS_Labels_I94CIT_I94RES.txt'

    :param spark_session: current spark session
    :param environment_variables: all available environment variables
    :return: staging table `st_immigration_countries` as data frame (df)
    """
    # get current environment variables
    location_to_read = environment_variables.get("st_immigration_countries_raw")

    # read source data
    df_st_immigration_countries_raw = spark_session \
        .read \
        .text(location_to_read)

    # create a new df with two columns (st_id_country_code, st_ic_country_name) as staging table
    # st_immigration_countries

    df_st_immigration_countries_raw = df_st_immigration_countries_raw \
        .select(sql_f.regexp_extract('value', r'^\s*(\d*)\s*=  \'(\w*.*)\'', 1).alias('st_ic_country_code')
                .cast(IntegerType()), sql_f.regexp_extract('value', r'^\s*(\d*)\s*=  \'(\w*.*)\'', 2)
                .alias('st_ic_country_name')) \
        .drop_duplicates() \
        .sort("st_ic_country_code")

    # Select default value for all invalid entries. All country_code entries in the fact table that are not included
    # in the st_immigration countries df get the country_code 999 as default value (invalid)
    df_st_immigration_countries_default = df_st_immigration_countries_raw \
        .select("st_ic_country_code", "st_ic_country_name") \
        .filter("st_ic_country_code ==  999")

    # data cleansing: filter out invalid values from staging table and drop them. Join default value back to dataframe
    df_st_immigration_countries = df_st_immigration_countries_raw \
        .select("st_ic_country_code",
                sql_f.regexp_replace('st_ic_country_name', r"^No Country Code (.*)", "EntryToDelete").alias(
                    "st_ic_country_name")
                ) \
        .select("st_ic_country_code",
                sql_f.regexp_replace('st_ic_country_name', r"^.*\((should not show)\)", "EntryToDelete").alias(
                    "st_ic_country_name")
                ) \
        .select("st_ic_country_code",
                sql_f.regexp_replace('st_ic_country_name', r"^INVALID:.*", "EntryToDelete").alias("st_ic_country_name")
                ) \
        .filter("st_ic_country_name != 'EntryToDelete'") \
        .union(df_st_immigration_countries_default)

    # return a data cleansed st_immigration_countries table
    return df_st_immigration_countries


def staging_table_create_st_immigration_airports(spark_session, environment_variables):
    """
    Create staging table st_immigration countries from file 'I94_SAS_Labels_I94PORT.txt'

    :param spark_session: current spark session
    :param environment_variables: all available environment variables
    :return: staging table `st_immigration_airports` as data frame (df)
    """
    # get current environment variables
    location_to_read = environment_variables.get("st_immigration_airports_raw")

    # read source data
    df_st_immigration_airports_raw = spark_session \
        .read \
        .text(location_to_read)

    # get regex_all values --> with errors like `Collapsed (BUF)` --> 660 Entries
    regex = r"^\s+'([.\w{2,3} ]*)'\s+=\s+'([\w -.\/]*)\s*,*\s* ([\w\/]+)"

    df_st_immigration_airports = df_st_immigration_airports_raw \
        .select(sql_f.regexp_extract('value', regex, 1).alias('st_ia_airport_code'),
                sql_f.regexp_extract('value', regex, 2).alias('st_ia_airport_name'),
                sql_f.regexp_extract('value', regex, 3).alias('st_ia_airport_state_code')) \
        .drop_duplicates() \
        .filter("st_ia_airport_code != ''") \
        .sort("st_ia_airport_state_code", "st_ia_airport_code")

    # correct all entries that are not error-free as expected (for more details check
    # file P8_Capstone_Project_Data_Preparation_Step_All.ipynb line 1016)

    # correct all entries that are not error-free as expected
    df_st_immigration_airports = df_st_immigration_airports \
        .select("st_ia_airport_code",
                sql_f.regexp_replace('st_ia_airport_name', r'Collapsed \(\w+\)|No PORT|UNKNOWN',
                                     'Invalid Airport Entry').alias("st_ia_airport_name"),
                sql_f.regexp_replace("st_ia_airport_state_code", r'06/15|Code|POE', 'Invalid State Code').alias(
                    "st_ia_airport_state_code")) \
        .select("st_ia_airport_code",
                sql_f.regexp_replace('st_ia_airport_name', r"^DERBY LINE,.*", "DERBY LINE, VT (RT. 5)").alias(
                    "st_ia_airport_name"),
                sql_f.regexp_replace("st_ia_airport_state_code", r"5", "VT").alias("st_ia_airport_state_code")) \
        .select("st_ia_airport_code",
                sql_f.regexp_replace('st_ia_airport_name', r"^LOUIS BOTHA, SOUTH", "LOUIS BOTHA").alias(
                    "st_ia_airport_name"),
                sql_f.regexp_replace("st_ia_airport_state_code", r"AFRICA", "SOUTH AFRICA").alias(
                    "st_ia_airport_state_code")) \
        .select("st_ia_airport_code",
                sql_f.regexp_replace('st_ia_airport_name', r",", "").alias("st_ia_airport_name"),
                "st_ia_airport_state_code") \
        .select("st_ia_airport_code",
                sql_f.regexp_replace('st_ia_airport_name', r"^PASO DEL", "PASO DEL NORTE").alias("st_ia_airport_name"),
                sql_f.regexp_replace("st_ia_airport_state_code", r"NORTE", "TX").alias("st_ia_airport_state_code")) \
        .select("st_ia_airport_code",
                sql_f.regexp_replace('st_ia_airport_name', r"^UNIDENTIFED AIR /?", "Invalid Airport Entry").alias(
                    "st_ia_airport_name"),
                sql_f.regexp_replace("st_ia_airport_state_code", r"^SEAPORT?", "Invalid State Code").alias(
                    "st_ia_airport_state_code")) \
        .select("st_ia_airport_code",
                sql_f.regexp_replace('st_ia_airport_name', r"Abu", "Abu Dhabi").alias("st_ia_airport_name"),
                sql_f.regexp_replace("st_ia_airport_state_code", r"Dhabi", "Invalid State Code").alias(
                    "st_ia_airport_state_code")) \
        .select("st_ia_airport_code",
                sql_f.regexp_replace('st_ia_airport_name', r"DOVER-AFB", "Invalid Airport Entry").alias(
                    "st_ia_airport_name"),
                sql_f.regexp_replace("st_ia_airport_state_code", r"DE", "Invalid State Code").alias(
                    "st_ia_airport_state_code")) \
        .select("st_ia_airport_code",
                sql_f.regexp_replace('st_ia_airport_name', r"NOT REPORTED/UNKNOWNGALES", "NOGALES").alias(
                    "st_ia_airport_name"),
                sql_f.regexp_replace("st_ia_airport_state_code", r"AZ", "AZ").alias("st_ia_airport_state_code")) \
        .select("st_ia_airport_code",
                sql_f.regexp_replace('st_ia_airport_name', r"^NOT", "Invalid Airport Entry").alias(
                    "st_ia_airport_name"),
                sql_f.regexp_replace("st_ia_airport_state_code", r"REPORTED/UNKNOWN", "Invalid State Code").alias(
                    "st_ia_airport_state_code")) \
        .select("st_ia_airport_code",
                sql_f.regexp_replace('st_ia_airport_name', r"INVALID - IWAKUNI", "IWAKUNI").alias("st_ia_airport_name"),
                sql_f.regexp_replace("st_ia_airport_state_code", r"JAPAN", "JAPAN").alias("st_ia_airport_state_code")) \
        .sort("st_ia_airport_name", "st_ia_airport_code")
    return df_st_immigration_airports


def staging_table_create_st_state_destinations(spark_session, environment_variables, staging_tables_available):
    """
    Clean data and create staging table `st_state_destinations` from file 'I94_SAS_Labels_I94ADDR.txt'.
    Extract some demographic data from file 'us-cities-demographics.json' like `age_median`, `population_male`,
    `population_female`, `population_total` or `foreign_born` and add them to staging table `st_state_destinations`.

    This table is created to answer Project Question: To which states in the U.S. do immigrants want to continue their
    travel after their initial arrival and what demographics can immigrants expect when they arrive in the destination
    state, such as average temperature, population numbers or population density?

    :param spark_session: current spark session
    :param environment_variables: all available environment variables
    :param staging_tables_available: already loaded staging tables into dict
    :return: staging table `st_state_destinations` as data frame (df)
    """
    # get current environment variables
    st_state_destinations_raw_i94_sas_labels_i94addr_path = environment_variables.get(
        "st_state_destinations_raw_i94_sas_labels_i94addr")
    st_state_destinations_raw_us_cities_demographics_path = environment_variables.get(
        "st_state_destinations_raw_us_cities_demographics")

    # get raw data
    df_st_i94_sas_labels_i94addr = spark_session.read.text(st_state_destinations_raw_i94_sas_labels_i94addr_path)

    # get regex_cleaned values -->
    regex_cleaned = r"^\s+'([9+A-Z]+)'='([A-Z\s.]+)'"

    df_st_i94_sas_labels_i94addr_regex_cleaned = df_st_i94_sas_labels_i94addr \
        .select(sql_f.regexp_extract('value', regex_cleaned, 1).alias('st_sd_state_code'),
                sql_f.regexp_extract('value', regex_cleaned, 2).alias('st_sd_state_name')) \
        .drop_duplicates() \
        .orderBy("st_sd_state_code")

    # get data from JSON-file
    df_us_cities_demographics = spark_session.read.json(st_state_destinations_raw_us_cities_demographics_path)

    # Get only values aggregated by state and not the city values.
    df_us_cities_demographics_agg = df_us_cities_demographics \
        .groupBy("fields.state_code", "fields.state") \
        .agg(sql_f.round(sql_f.avg('fields.median_age'), 1).alias('st_sd_age_median')
             , sql_f.round(sql_f.avg('fields.male_population').cast(IntegerType()), 2).alias('st_sd_population_male')
             ,
             sql_f.round(sql_f.avg('fields.female_population').cast(IntegerType()), 2).alias('st_sd_population_female')
             , sql_f.round(sql_f.avg('fields.total_population').cast(IntegerType()), 2).alias('st_sd_population_total')
             , sql_f.round(sql_f.avg('fields.foreign_born').cast(IntegerType()), 2).alias('st_sd_foreign_born')
             ) \
        .orderBy("fields.state_code") \
        .withColumnRenamed("fields.state_code", "state_code") \
        .withColumnRenamed("fields.state", "state")

    # Join "df_st_I94_SAS_Labels_I94ADDR_regex_cleaned" and "df_us_cities_demographics" to get new data frame
    # "df_st_state_destinations" fill up null values with 0
    df_st_state_destinations = df_st_i94_sas_labels_i94addr_regex_cleaned \
        .join(df_us_cities_demographics_agg, df_st_i94_sas_labels_i94addr_regex_cleaned.st_sd_state_code ==
              df_us_cities_demographics_agg.state_code, 'left') \
        .drop("state_code", "state") \
        .withColumn("st_sd_state_name", sql_f.initcap(sql_f.col("st_sd_state_name"))) \
        .fillna(value=0.0, subset=['st_sd_age_median']) \
        .fillna(value=0, subset=['st_sd_population_male']) \
        .fillna(value=0, subset=['st_sd_population_female']) \
        .fillna(value=0, subset=['st_sd_population_total']) \
        .fillna(value=0, subset=['st_sd_foreign_born'])

    return df_st_state_destinations


# Create new data frame with date series
def generate_dates(spark_session, range_list, dt_col="date_time_ref",
                   interval=60 * 60 * 24):
    """
    ...     Create a Spark DataFrame with a single column named dt_col and a range of date within a specified interval
    ...      (start and stop included).
    ...     With hourly data, dates end at 23 of stop day
    ...     (https://stackoverflow.com/questions/57537760/pyspark-how-to-generate-a-dataframe-composed-of-datetime-range)
    ...
    ...     :param spark: SparkSession or sqlContext depending on environment (server vs local)
    ...     :param range_list: array of strings formatted as "2018-01-20" or "2018-01-20 00:00:00"
    ...     :param interval: number of seconds (frequency), output from get_freq()
    ...     :param dt_col: string with date column name. Date column must be TimestampType
    ...
    ...     :returns: df from range
    ...     """
    start, stop = range_list
    temp_df = spark_session.createDataFrame([(start, stop)], ("start", "stop"))
    temp_df = temp_df.select([sql_f.col(c).cast("timestamp") for c in ("start", "stop")])
    temp_df = temp_df.withColumn("stop", sql_f.date_add("stop", 1).cast("timestamp"))
    temp_df = temp_df.select([sql_f.col(c).cast("long") for c in ("start", "stop")])
    start, stop = temp_df.first()

    return spark_session.range(start, stop, interval).select(
        sql_f.col("id").cast("timestamp").cast("date").alias(dt_col))


def staging_table_create_st_date_arrivals(spark_session, environment_variables, staging_tables_available):
    """**Date dimension**
    `st_i94_arrdate` from staging table `st_i94_immigration` describes a date in SAS specific Date format.
    The SAS date calculation starts on 1960-01-01. This column is converted to DateType format in the staging table
    `st_i94_immigrations` as column named `st_i94_arrdate_iso`.

    Generate new date staging table (`st_date_arrivals`) based on default, min and max values

    :param spark_session: current spark session
    :param environment_variables: all available environment variables
    :param staging_tables_available: already loaded staging tables into dict
    :return: staging table `st_date_arrivals` as data frame (df)
    """

    # get data frame df_st_i94_immigrations to extract the min and max arrdate
    table_to_load_variable = "st_i94_immigrations"
    staging_table_to_load_name = environment_variables.get(f"{table_to_load_variable}_name")
    df_st_i94_immigrations = get_dataframe(spark_session, environment_variables, staging_tables_available,
                                           staging_table_to_load_name)

    # Get min and max values for "st_i94_arrdate"
    st_i94_arrdate_iso_min, st_i94_arrdate_iso_max = df_st_i94_immigrations \
        .select(sql_f.min("st_i94_arrdate_iso").alias("st_i94_arrdate_iso_min"),
                sql_f.max("st_i94_arrdate_iso").alias("st_i94_arrdate_iso_max")) \
        .first()

    # create new staging table "st_date_arrivals"
    date_range = [st_i94_arrdate_iso_min, st_i94_arrdate_iso_max]
    dt_col = "st_da_date"
    df_st_date_arrivals = generate_dates(spark_session, date_range, dt_col)

    # create new columns of st_date_arrivals table
    # https://spark.apache.org/docs/latest/sql-ref-datetime-pattern.html

    df_st_date_arrivals = df_st_date_arrivals \
        .withColumn("st_da_id", sql_f.col("st_da_date")) \
        .withColumn("st_da_year", sql_f.year(sql_f.col("st_da_date"))) \
        .withColumn("st_da_year_quarter",
                    sql_f.concat_ws('/', sql_f.year(sql_f.col("st_da_date")), sql_f.quarter(sql_f.col("st_da_date")))) \
        .withColumn("st_da_year_month",
                    sql_f.concat_ws('/', sql_f.year(sql_f.col("st_da_date")), sql_f.month(sql_f.col("st_da_date")))) \
        .withColumn("st_da_year_month",
                    sql_f.concat_ws('/', sql_f.year(sql_f.col("st_da_date")),
                                    sql_f.date_format(sql_f.col("st_da_date"), 'MM'))) \
        .withColumn("st_da_quarter", sql_f.quarter(sql_f.col("st_da_date"))) \
        .withColumn("st_da_month", sql_f.month(sql_f.col("st_da_date"))) \
        .withColumn("st_da_week", sql_f.weekofyear(sql_f.col("st_da_date"))) \
        .withColumn("st_da_weekday", sql_f.date_format(sql_f.col("st_da_date"), 'EEEE')) \
        .withColumn("st_da_weekday_short", sql_f.date_format(sql_f.col("st_da_date"), 'EEE')) \
        .withColumn("st_da_dayofweek", sql_f.dayofweek(sql_f.col("st_da_date"))) \
        .withColumn("st_da_day", sql_f.dayofmonth(sql_f.col("st_da_date")))

    return df_st_date_arrivals


def staging_table_create_st_date_departures(spark_session, environment_variables, staging_tables_available):
    """**Date dimension**
    `st_i94_depdate` from staging table `st_i94_immigration` describes a date in SAS specific Date format.
    The SAS date calculation starts on 1960-01-01. This column is converted to DateType format in the staging table
    `st_i94_immigrations` as column named `st_i94_depdate_iso`.

    Generate new date staging table (`st_date_departures`) based on default, min and max values

    :param spark_session: current spark session
    :param environment_variables: all available environment variables
    :param staging_tables_available: already loaded staging tables into dict
    :return: staging table `st_date_departures` as data frame (df)
    """
    # get data frame df_st_i94_immigrations to extract the min and max depdate
    table_to_load_variable = "st_i94_immigrations"
    staging_table_to_load_name = environment_variables.get(f"{table_to_load_variable}_name")
    df_st_i94_immigrations = get_dataframe(spark_session, environment_variables, staging_tables_available,
                                           staging_table_to_load_name)

    # Extract default, min and max date from column 'st_i94_depdate_iso'
    # get default and min value
    st_i94_depdate_iso_default, st_i94_depdate_iso_min = df_st_i94_immigrations \
        .select("st_i94_depdate_iso") \
        .distinct() \
        .orderBy("st_i94_depdate_iso", ascending=True) \
        .limit(2) \
        .select(sql_f.min("st_i94_depdate_iso").alias("st_i94_depdate_iso_default"),
                sql_f.max("st_i94_depdate_iso").alias("st_i94_depdate_iso_min")) \
        .first()

    # get max value
    st_i94_depdate_iso_max, st_i94_depdate_iso_max = df_st_i94_immigrations \
        .select(sql_f.max("st_i94_depdate_iso").alias("st_i94_depdate_iso_max"), \
                sql_f.max("st_i94_depdate_iso").alias("st_i94_depdate_iso_max")) \
        .first()

    # create new staging table "st_date_departures"
    date_range_default = [st_i94_depdate_iso_default, st_i94_depdate_iso_default]
    date_range_min_max = [st_i94_depdate_iso_min, st_i94_depdate_iso_max]

    # create new data frames for
    dt_col = "st_dd_date"
    df_st_date_departures_default = generate_dates(spark_session, date_range_default, dt_col)
    df_st_date_departures_min_max = generate_dates(spark_session, date_range_min_max, dt_col)

    # combine both data frames to append `1900-01-01` to all other dates
    df_st_date_departures = df_st_date_departures_default.union(df_st_date_departures_min_max)

    # Append date specific columns to staging table `st_date_departures`.
    # create new columns of st_date_departures table
    # https://spark.apache.org/docs/latest/sql-ref-datetime-pattern.html

    df_st_date_departures = df_st_date_departures \
        .withColumn("st_dd_id", sql_f.col("st_dd_date")) \
        .withColumn("st_dd_year", sql_f.year(sql_f.col("st_dd_date"))) \
        .withColumn("st_dd_year_quarter",
                    sql_f.concat_ws('/', sql_f.year(sql_f.col("st_dd_date")), sql_f.quarter(sql_f.col("st_dd_date")))) \
        .withColumn("st_dd_year_month",
                    sql_f.concat_ws('/', sql_f.year(sql_f.col("st_dd_date")),
                                    sql_f.date_format(sql_f.col("st_dd_date"), "MM"))) \
        .withColumn("st_dd_quarter", sql_f.quarter(sql_f.col("st_dd_date"))) \
        .withColumn("st_dd_month", sql_f.month("st_dd_date")) \
        .withColumn("st_dd_week", sql_f.weekofyear(sql_f.col("st_dd_date"))) \
        .withColumn("st_dd_weekday", sql_f.date_format(sql_f.col("st_dd_date"), 'EEEE')) \
        .withColumn("st_dd_weekday_short", sql_f.date_format(sql_f.col("st_dd_date"), 'EEE')) \
        .withColumn("st_dd_dayofweek", sql_f.dayofweek(sql_f.col("st_dd_date"))) \
        .withColumn("st_dd_day", sql_f.dayofmonth(sql_f.col("st_dd_date")))

    return df_st_date_departures


def dimension_table_create_d_immigration_countries(spark_session, environment_variables, staging_tables_available):
    """
    Create dimension table d_immigration_countries based of staging table st_i94_immigration_countries

    :param spark_session: current spark session
    :param environment_variables: all available environment variables
    :param staging_tables_available: already loaded staging tables into dict
    :return: dimension table `d_immigration_countries` as data frame (df)
    """
    # get available staging table
    table_to_load_variable = "st_immigration_countries"
    staging_table_to_load_name = environment_variables.get(f"{table_to_load_variable}_name")
    df_st_immigration_countries = get_dataframe(spark_session, environment_variables, staging_tables_available,
                                                staging_table_to_load_name)

    # rename staging columns to valid dimension columns
    df_d_immigration_countries = df_st_immigration_countries \
        .withColumn("d_ic_id", sql_f.col("st_ic_country_code")) \
        .withColumnRenamed("st_ic_country_code", "d_ic_country_code") \
        .withColumnRenamed("st_ic_country_name", "d_ic_country_name")

    return df_d_immigration_countries


def dimension_table_create_d_immigration_airports(spark_session, environment_variables, staging_tables_available):
    """
    Create dimension table d_immigration_airports based of staging table st_i94_immigration_airports

    :param spark_session: current spark session
    :param environment_variables: all available environment variables
    :param staging_tables_available: already loaded staging tables into dict
    :return: dimension table `d_immigration_airports` as data frame (df)
    """
    # get available staging table
    table_to_load_variable = "st_immigration_airports"
    staging_table_to_load_name = environment_variables.get(f"{table_to_load_variable}_name")
    df_st_immigration_airports = get_dataframe(spark_session, environment_variables, staging_tables_available,
                                               staging_table_to_load_name)

    # add Primary Key and rename columns from staging table to valid dimension columns
    df_d_immigration_airports = df_st_immigration_airports \
        .withColumn("d_ia_id", df_st_immigration_airports.st_ia_airport_code) \
        .withColumnRenamed("st_ia_airport_code", "d_ia_airport_code") \
        .withColumnRenamed("st_ia_airport_name", "d_ia_airport_name") \
        .withColumnRenamed("st_ia_airport_state_code", "d_ia_airport_state_code")

    return df_d_immigration_airports


def dimension_table_create_d_date_arrivals(spark_session, environment_variables, staging_tables_available):
    """**Date dimension**
    `st_i94_arrdate` from staging table `st_i94_immigration` describes a date in SAS specific Date format.
    The SAS date calculation starts on 1960-01-01. This column is converted to DateType format in the staging table
    `st_i94_immigrations` as column named `st_i94_arrdate_iso`.

    Generate new date dimension table (`d_date_arrivals`) based on default, min and max values

    :param spark_session: current spark session
    :param environment_variables: all available environment variables
    :param staging_tables_available: already loaded staging tables into dict
    :return: dimension table `d_date_arrivals` as data frame (df)
    """

    # get data frame df_st_date_arrivals to extract the min and max arrdate
    table_to_load_variable = "st_date_arrivals"
    staging_table_to_load_name = environment_variables.get(f"{table_to_load_variable}_name")
    df_st_date_arrivals = get_dataframe(spark_session, environment_variables, staging_tables_available,
                                        staging_table_to_load_name)

    # rename staging columns to valid dimension columns
    df_d_date_arrivals = df_st_date_arrivals \
        .withColumnRenamed("st_da_date", "d_da_date") \
        .withColumnRenamed("st_da_id", "d_da_id") \
        .withColumnRenamed("st_da_year", "d_da_year") \
        .withColumnRenamed("st_da_year_quarter", "d_da_year_quarter") \
        .withColumnRenamed("st_da_year_month", "d_da_year_month") \
        .withColumnRenamed("st_da_quarter", "d_da_quarter") \
        .withColumnRenamed("st_da_month", "d_da_month") \
        .withColumnRenamed("st_da_week", "d_da_week") \
        .withColumnRenamed("st_da_weekday", "d_da_weekday") \
        .withColumnRenamed("st_da_weekday_short", "d_da_weekday_short") \
        .withColumnRenamed("st_da_dayofweek", "d_da_dayofweek") \
        .withColumnRenamed("st_da_day", "d_da_day")

    return df_d_date_arrivals


def dimension_table_create_d_date_departures(spark_session, environment_variables, staging_tables_available):
    """**Date dimension**
    `st_i94_depdate` from staging table `st_i94_immigration` describes a date in SAS specific Date format.
    The SAS date calculation starts on 1960-01-01. This column is converted to DateType format in the staging table
    `st_i94_immigrations` as column named `st_i94_depdate_iso`.

    Generate new date dimension table (`d_date_departures`) based on default, min and max values

    :param spark_session: current spark session
    :param environment_variables: all available environment variables
    :param staging_tables_available: already loaded staging tables into dict
    :return: dimension table `d_date_departures` as data frame (df)
    """
    # get data frame df_d_departures to extract the min and max depdate
    table_to_load_variable = "st_date_departures"
    staging_table_to_load_name = environment_variables.get(f"{table_to_load_variable}_name")
    df_st_date_departures = get_dataframe(spark_session, environment_variables, staging_tables_available,
                                          staging_table_to_load_name)

    # rename staging columns to valid dimension columns
    df_d_date_departures = df_st_date_departures \
        .withColumnRenamed("st_dd_date", "d_dd_date") \
        .withColumnRenamed("st_dd_id", "d_dd_id") \
        .withColumnRenamed("st_dd_year", "d_dd_year") \
        .withColumnRenamed("st_dd_year_quarter", "d_dd_year_quarter") \
        .withColumnRenamed("st_dd_year_month", "d_dd_year_month") \
        .withColumnRenamed("st_dd_quarter", "d_dd_quarter") \
        .withColumnRenamed("st_dd_month", "d_dd_month") \
        .withColumnRenamed("st_dd_week", "d_dd_week") \
        .withColumnRenamed("st_dd_weekday", "d_dd_weekday") \
        .withColumnRenamed("st_dd_weekday_short", "d_dd_weekday_short") \
        .withColumnRenamed("st_dd_dayofweek", "d_dd_dayofweek") \
        .withColumnRenamed("st_dd_day", "d_dd_day")

    return df_d_date_departures


def dimension_table_create_d_state_destinations(spark_session, environment_variables, staging_tables_available):
    """
    Generate new dimension table (`d_state_destinations`) based on staging table `st_state_destinations`.

    :param spark_session: current spark session
    :param environment_variables: all available environment variables
    :param staging_tables_available: already loaded staging tables into dict
    :return: dimension table `d_state_destinations` as data frame (df)
    """
    # get data frame df_d_departures to extract the min and max depdate
    table_to_load_variable = "st_state_destinations"
    staging_table_to_load_name = environment_variables.get(f"{table_to_load_variable}_name")
    df_st_state_destinations = get_dataframe(spark_session, environment_variables, staging_tables_available,
                                             staging_table_to_load_name)

    # rename staging columns to valid dimension columns
    df_d_state_destinations = df_st_state_destinations \
        .withColumn("d_sd_id", sql_f.col("st_sd_state_code")) \
        .withColumnRenamed("st_sd_state_code", "d_sd_state_code") \
        .withColumnRenamed("st_sd_state_name", "d_sd_state_name") \
        .withColumnRenamed("st_sd_age_median", "d_sd_age_median") \
        .withColumnRenamed("st_sd_population_male", "d_sd_population_male") \
        .withColumnRenamed("st_sd_population_female", "d_sd_population_female") \
        .withColumnRenamed("st_sd_population_total", "d_sd_population_total") \
        .withColumnRenamed("st_sd_foreign_born", "d_sd_foreign_born")

    return df_d_state_destinations


def fact_table_create_f_i94_immigrations(spark_session, environment_variables, staging_tables_available):
    """
    Generate fact table (`f_i94_immigrations`) based on staging table `st_i94_immigrations`.

    :param spark_session: current spark session
    :param environment_variables: all available environment variables
    :param staging_tables_available: already loaded staging tables into dict
    :return: fact table `f_i94_immigrations` as dataframe (df)
    """
    # get data frame df_st_i94_immigrations and Foreign Key's to dimension tables
    table_to_load_variable = "st_i94_immigrations"
    staging_table_to_load_name = environment_variables.get(f"{table_to_load_variable}_name")
    df_st_i94_immigrations = get_dataframe(spark_session, environment_variables, staging_tables_available,
                                           staging_table_to_load_name)

    table_to_load_variable = "d_state_destinations"
    dimension_table_to_load_name = environment_variables.get(f"{table_to_load_variable}_name")
    df_d_state_destinations = get_dataframe(spark_session, environment_variables, staging_tables_available,
                                            dimension_table_to_load_name)

    # add / rename columns to create the fact table
    df_f_i94_immigrations = df_st_i94_immigrations \
        .drop('st_i94_arrdate', 'st_i94_depdate') \
        .withColumnRenamed('st_i94_cit', 'f_i94_cit') \
        .withColumnRenamed('st_i94_addr', 'f_i94_addr') \
        .withColumnRenamed('st_i94_arrdate_iso', 'f_i94_arrdate_iso') \
        .withColumnRenamed('st_i94_depdate_iso', 'f_i94_depdate_iso') \
        .withColumnRenamed('st_i94_dtadfile', 'f_i94_dtadfile') \
        .withColumnRenamed('st_i94_matflag', 'f_i94_matflag') \
        .withColumnRenamed('st_i94_count', 'f_i94_count') \
        .withColumnRenamed('st_i94_id', 'f_i94_id') \
        .withColumnRenamed('st_i94_port_state_code', 'f_i94_port_state_code') \
        .withColumnRenamed('st_i94_year', 'f_i94_year') \
        .withColumnRenamed('st_i94_month', 'f_i94_month') \
        .withColumnRenamed('st_i94_port', 'f_i94_port') \
        .withColumn("d_ia_id", sql_f.col("f_i94_port")) \
        .withColumn("d_sd_id", sql_f.col("f_i94_addr")) \
        .withColumn("d_da_id", sql_f.col("f_i94_arrdate_iso")) \
        .withColumn("d_dd_id", sql_f.col("f_i94_depdate_iso")) \
        .withColumn("d_ic_id", sql_f.col("f_i94_cit")) \
        .select('f_i94_id',
                'd_ia_id',
                'd_sd_id',
                'd_da_id',
                'd_dd_id',
                'd_ic_id',
                'f_i94_cit',
                'f_i94_addr',
                'f_i94_arrdate_iso',
                'f_i94_depdate_iso',
                'f_i94_dtadfile',
                'f_i94_matflag',
                'f_i94_count',
                'f_i94_year',
                'f_i94_month',
                'f_i94_port',
                'f_i94_port_state_code'
                )

    # prepare dataframe `df_f_i94_immigrations_2_join` to get only the allowed state codes to prepare the FK for the
    # fact table f_i94_immigrations
    df_f_i94_immigrations_2_join = df_d_state_destinations \
        .select("d_sd_id") \
        .withColumnRenamed("d_sd_id", "d_sd_id_reference") \
        .orderBy("d_sd_id_reference")

    # clean column "f_i94_immigrations.d_sd_id" by column "d_sd_id_cleaned (d_sd_id_reference)"
    # Content of existing column "d_sd_id" will be overwritten.
    df_f_i94_immigrations = df_f_i94_immigrations \
        .join(df_f_i94_immigrations_2_join,
              df_f_i94_immigrations_2_join.d_sd_id_reference == df_f_i94_immigrations.d_sd_id, 'left') \
        .withColumn("d_sd_id", sql_f.when(sql_f.col("d_sd_id_reference").isNull(), "99")
                    .otherwise(sql_f.col("d_sd_id_reference"))) \
        .drop("d_sd_id_reference")

    return df_f_i94_immigrations


def project_question_to_answer(spark_session, environment_variables, data_tables_available,
                               project_question_to_answer_variable):
    """
     Answer project questions configured in the configuration file (*.cfg). The result is output to the console. The
     whole dataset is exported to a file in JSON format.

    :param spark_session: current spark session
    :param environment_variables: all available environment variables
    :param data_tables_available: already loaded tables into dict
    :param project_question_to_answer_variable: name of project question to get environment variable
    """
    # set environment variables
    pq_name = environment_variables.get(f"{project_question_to_answer_variable}_name")
    pq_question = environment_variables.get(f"{project_question_to_answer_variable}_question")

    # Console output for information
    print()
    print(f"{pq_name}: {pq_question} ==> Processing started ...")
    print()

    # get resources as dataframe and register them as TempViews to answer the project questions
    dataframes_to_load = environment_variables.get(f"{project_question_to_answer_variable}_resources").split(sep=',')
    for df_to_load in dataframes_to_load:
        # get dataframes
        current_dataframe = get_dataframe(spark_session, environment_variables, data_tables_available, df_to_load)
        # register as TempView
        current_dataframe.createOrReplaceTempView(df_to_load)
        print(f"Dataframe registered as TempView: {df_to_load}")

    # SQL to answer current project question
    df_sql = environment_variables.get(f"{project_question_to_answer_variable}_sql")
    df_pq = spark_session.sql(f"""{df_sql}""")

    # Print to console:
    print()
    print("Answer of ")
    print(f"{pq_name}: {pq_question}")
    print()
    df_pq.show(20, False)

    # persist dataframe to JSON format
    persist_data_frame_json(spark_session, environment_variables, project_question_to_answer_variable, df_pq)


def data_quality_checks(spark_session, environment_variables, data_tables_available):
    """
    The data quality checks are to ensure the pipeline ran as expected. This includes:
    * Check if key fields have valid values (no nulls or empty)
    * Check that table has > 0 rows

    :param spark_session: current spark session
    :param environment_variables: all available environment variables
    :param data_tables_available: already loaded tables into dict
    """

    # #### Initialization ####

    # Define format to store data quality result data frame
    result_struct_type = StructType(
        [
            StructField("dq_result_table_name", StringType(), True)
            , StructField("dq_result_null_entries", IntegerType(), True)
            , StructField("dq_result_entries", IntegerType(), True)
            , StructField("dq_result_status", StringType(), True)
        ]
    )
    # create results data frame
    dq_results = []
    df_dq_results = spark_session.createDataFrame(dq_results, result_struct_type)

    # print some output to console
    print()
    print(f"Data Quality Check(s):")

    # read available tables to check data quality
    for current_table in data_tables_available.keys():

        # ### There is NO check for STAGING tables !!! These tables will be ignored
        # DQ check is executed ONLY if there is an entry in the configuration file ({current_table}_dq_column)

        # get value from configuration file
        dq_columns_available = environment_variables.get(f"{current_table}_dq_column")
        if dq_columns_available:

            print()
            print(f"Table: {current_table}")

            dq_current_table_check_null_values = environment_variables.get(f"{current_table}_dq_column").split(sep=',')

            # get data table
            df_dq_current_table = get_dataframe(spark_session, environment_variables, data_tables_available,
                                                current_table)

            # set default value
            dq_current_table_check_null_values_result = 0

            # loop thru all columns to check which are configured in config file
            for dq_column_check_null_values in dq_current_table_check_null_values:
                # Check if key fields have valid values (no nulls or empty)
                dq_column_check_null_values_result = df_dq_current_table \
                    .select(f"{dq_column_check_null_values}") \
                    .where(f"{dq_column_check_null_values} is null or {dq_column_check_null_values} == ''") \
                    .count()

                # if result is grater 0 then you have some failures
                dq_current_table_check_null_values_result += dq_column_check_null_values_result

                # Print some output to console
                print(f"Null values in column: {dq_column_check_null_values} --> Result: "
                      f"{dq_column_check_null_values_result}")

            # Check if current table has > 0 rows
            df_dq_check_content = df_dq_current_table.count()

            # insert result into result_df
            if dq_current_table_check_null_values_result < 1 and df_dq_check_content > 0:
                dq_check_result = "OK"
            else:
                dq_check_result = "NOK"

            # Print out result of dq check
            print(f"DQ-Check result: {dq_check_result}")

            # content for result dataframe
            dq_results = [(current_table, dq_current_table_check_null_values_result, df_dq_check_content,
                           dq_check_result)]

            # add new row to current results data frame
            new_row = spark_session.createDataFrame(dq_results, result_struct_type)
            df_dq_results = df_dq_results.union(new_row)

        else:
            print()
            print(f"Table: {current_table}")
            print(f"DQ-Check result: There is no configuration entry for a Data Quality check!")
            print()

    # get result table and print it to the console
    if df_dq_results.count() > 0:
        print()
        print(f"Result of Data Quality Checks:")
        lines_to_show = df_dq_results.count()
        df_dq_results.show(lines_to_show, False)


# create data dictionary of used star schema
def create_data_dictionary_from_df(spark_session, environment_variables, data_tables_available):
    """
    Generate a fully data dictionary automatically from available in dict 'data_tables_available'

    :param spark_session: current spark session
    :param environment_variables: all available environment variables
    :param data_tables_available: already loaded tables into dict
    """
    json_table_data = {}
    tables = {}

    print("Data Dictionary creation started ...")
    # tables_to_describe = environment_variables.get("dd_dictionary_tables").split(sep=",")
    tables_to_describe = data_tables_available.keys()

    # loop thru list of tables (df) to describe (star schema)
    for current_table_name_from_df in tables_to_describe:

        # Set current table name. Table description will be filled in later. Columns will be appended later also.
        current_table_description = environment_variables.get(
            f"tables__{current_table_name_from_df}__table_description")

        # if table description is configured in config (*.cfg) file --> fill information
        if current_table_description:
            dict_current_table = {"table_name": current_table_name_from_df,
                                  "table_description": f"{current_table_description}"}
        else:
            dict_current_table = {"table_name": current_table_name_from_df,
                                  "table_description": "not set"}
        # read all table columns for current table from df
        current_table_columns_df = [get_dataframe(spark_session, environment_variables, data_tables_available,
                                                  current_table_name_from_df).columns]

        # create dictionary from table columns
        current_table_columns_dict = {}

        # loop thru list "current_table_columns_df" and add columns to dict "current_table_columns_dict"
        for counter, current_table_columns_df_column in enumerate(current_table_columns_df, start=1):
            for current_column in enumerate(current_table_columns_df_column, start=1):

                # add descriptions to data dictionary for tables and table columns
                current_column_description = environment_variables.get(
                    f"tables__{current_table_name_from_df}__columns__{current_column[1]}__column_description")

                # if column description is configured in config (*.cfg) file --> fill information
                if current_column_description:
                    current_table_column_name_dict = {"column_name": current_column[counter],
                                                      "column_description": f"{current_column_description}"}
                else:
                    current_table_column_name_dict = {"column_name": current_column[counter],
                                                      "column_description": "not set"}

                current_table_columns_dict[current_column[counter]] = current_table_column_name_dict

        dict_current_table["columns"] = current_table_columns_dict

        tables[current_table_name_from_df] = dict_current_table

        # add tables content to the dict json_data
        json_table_data["tables"] = tables

    # configure environment variables to persist current data frame
    dataframe_to_persist_name = "data_dictionary"
    dataframe_to_persist = environment_variables.get(f"{dataframe_to_persist_name}_name")
    dataframe_to_persist_file = dataframe_to_persist + ".json"
    dataframe_to_persist_path = environment_variables.get(f"{dataframe_to_persist_name}_path_json")
    dataframe_to_persist_full_path_json = path.join(dataframe_to_persist_path, dataframe_to_persist_file)

    # if folder does not exist, create it!
    if not path.exists(dataframe_to_persist_path):
        folder_to_create(dataframe_to_persist_path)

    with open(dataframe_to_persist_full_path_json, "w") as outfile:
        json.dump(json_table_data, outfile, sort_keys=True, indent=4, ensure_ascii=False)

        # persist data to file in json format
    """    if not path.exists(dataframe_to_persist_path):
            folder_to_create(dataframe_to_persist_path)
        else:"""

    print()
    print(f"Data Dictionary created. File is located at: {dataframe_to_persist_full_path_json}")


def main():
    """
    This project works with a data set for immigration to the United States. Based on the given data set, the following
    four project questions (PQ) are posed for business analysis, which need to be answered in this project. The data
    pipeline and star data model are completely aligned with the following questions.

    Project Questions:
    1. From which country do immigrants come to the U.S. and how many?
    2. At what airports do foreign persons arrive for immigration to the U.S.?
    3. At what times do foreign persons arrive for immigration to the U.S.?
    4. To which states in the U.S. do immigrants want to continue their travel after their initial arrival and what
       demographics can immigrants expect when they arrive in the destination state, such as average temperature,
       population, numbers or population density?
    """

    # ####### Initialization #######
    # "time_tracker" - timestamp to track running time
    total_execution_start = datetime.now()
    print("Started main(): " + str(total_execution_start))

    # set up environment variables
    environment_variables = set_up_environment_variables()
    data_tables_available = {}

    # crate a spark session
    spark_session = create_spark_session()

    # ####### Prepare raw data #######
    # convert I94 files from SAS into parquet format
    convert_i94_sas_data_into_parquet_staging_files(spark_session, environment_variables)

    # ####### Answer project questions #######
    # ### Step 3.1.1._4.1.1. #### Project question 1 --> From which country do immigrants come to the U.S. and how many?

    # ### Step 3.1.2._4.1.2. #### Project question 2 --> At what airports do foreign persons arrive for immigration to
    # the U.S.?

    # ### Step 3.1.3._4.1.3. #### Project question 3 --> At what times do foreign persons arrive for immigration to the
    # U.S.?

    # ### Step 3.1.4._4.1.4. #### Project question 4 --> To which states in the U.S. do immigrants want to continue
    # their travel after their initial arrival and what demographics can immigrants expect when they arrive in the
    # destination state, such as average temperature, population numbers or population density?

    project_questions = ['pq1', 'pq2', 'pq3', 'pq4']
    for pq_current in project_questions:
        project_question_to_answer_name = pq_current
        project_question_to_answer(spark_session, environment_variables, data_tables_available,
                                   project_question_to_answer_name)

    # Data Quality Section (Step 4.2) - Execution of data quality checks
    data_quality_checks(spark_session, environment_variables, data_tables_available)

    # Data dictionary (Step 4.3) - Creation of a data dictionary complete automatically. Output format: JSON
    create_data_dictionary_from_df(spark_session, environment_variables, data_tables_available)

    # "time_tracker"
    total_execution_end = datetime.now()
    time_tracker("P8_Capstone_Project_Data_Preparation.py/main()", total_execution_start, total_execution_end)


if __name__ == "__main__":
    main()
