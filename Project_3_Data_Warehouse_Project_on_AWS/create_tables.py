import configparser
import psycopg2
from sql_queries import drop_schema_for_project3_query, create_schema_and_search_path, create_table_queries, \
    drop_table_queries

"""
This part is used to prepare an own schema for PROJECT3 

Info: The drop_tables() procedure is not necessary to execute, because the command above will drop the whole schema
"""


# Prepare schema for work in Project 3
def drop_schema_for_project3(cur, conn):
    for query in drop_schema_for_project3_query:
        cur.execute(query)
        conn.commit()


# Create schema and switch search path to "Project3"
def prepare_schema_and_search_path(cur, conn):
    for query in create_schema_and_search_path:
        cur.execute(query)
        conn.commit()


# Drop all tables to prepare a clean environment
# ---> normally not necessary, because of the DROP SCHEMA ... CASCADE command <---
def drop_tables(cur, conn):
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


# Create tables for the project
def create_tables(cur, conn):
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


"""
Parts of the current table preparation script
- Initialization (Read configuration from file)
- Get DB Connection to AWS
- Schema preparation for Project3
- Clean up old tables for a clean environment (not necessary because of the schema deletion)
- Create clean table structure
- Close DB connection 
"""


def main():
    # Initialize environment and read configuration file
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    print("Initialized environment and read configuration file")

    # Establish DB-Connection to AWS Redshift
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    print("Established DB-Connection to AWS Redshift")

    # Prepare schema for work in Project 3
    drop_schema_for_project3(cur, conn)
    prepare_schema_and_search_path(cur, conn)
    print("Prepared schema for work in Project 3")

    # Drop all tables to prepare a clean environment
    # ---> normally not necessary, because of the DROP SCHEMA ... CASCADE command <---
    drop_tables(cur, conn)
    print("Dropped all tables to prepare a clean environment")

    # Create tables for the project
    create_tables(cur, conn)
    print("Created tables for the project")

    # clean up and close existing connection
    conn.close()
    print("DB-Connection to AWS Redshift closed")


if __name__ == "__main__":
    main()
