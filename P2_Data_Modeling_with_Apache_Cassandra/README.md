# Project 2 - Data Modeling with Apache Cassandra
> by Peter Wissel
>> 2020-05-14

# Project: Data Modeling with Cassandra

A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new 
music streaming app. The analysis team is particularly interested in understanding what songs users are listening to. 
Currently, there is no easy way to query the data to generate the results, since the data reside in a directory of CSV 
files on user activity on the app.

They'd like a data engineer (me) to create an Apache Cassandra database which can create queries on song play data to 
answer the questions, and wish to bring me on the project. My role is to create a database for this analysis. I'll be able 
to test my database by running queries given to me by the analytics team from Sparkify to create the results.

# Project Overview

In this project, I'll apply what I've learned on data modeling with Apache Cassandra and complete an ETL pipeline 
using Python. To complete the project, I will need to model my data by creating tables in Apache Cassandra to run 
queries. I'm provided with part of the ETL pipeline that transfers data from a set of CSV files within a directory 
to create a streamlined CSV file to model and insert data into Apache Cassandra tables.

They provided me with a project template that takes care of all the imports and provides a structure for ETL 
pipeline I'd need to process this data.

#Datasets

For this project, you'll be working with one dataset: event_data. The directory of CSV files partitioned by date. Here 
are examples of filepaths to two files in the dataset:

    event_data/2018-11-08-events.csv
    event_data/2018-11-09-events.csv

# Project Template

To get started with the project, go to the workspace on the next page, where you'll find the project template (a Jupyter 
notebook file). You can work on your project and submit your work through this workspace.

The project template includes one Jupyter Notebook file, in which:

- you will process the `event_datafile_new.csv` dataset to create a denormalized dataset
- you will model the data tables keeping in mind the queries you need to run
- you have been provided queries that you will need to model your data tables for
- you will load the data into tables you create in Apache Cassandra and run your queries

# Project Steps

Below are steps you can follow to complete each component of this project.

## Modeling your NoSQL database or Apache Cassandra database

1. Design tables to answer the queries outlined in the project template
2. Write Apache Cassandra `CREATE KEYSPACE` and `SET KEYSPACE` statements
3. Develop your `CREATE` statement for each of the tables to address each question
4. Load the data with `INSERT` statement for each of the tables
5. Include `IF NOT EXISTS` clauses in your `CREATE` statements to create tables only if the tables do not already exist. 
We recommend you also include `DROP TABLE` statement for each table, this way you can run drop and create tables 
whenever you want to reset your database and test your ETL pipeline
6. Test by running the proper select statements with the correct `WHERE` clause

## Build ETL Pipeline

1. Implement the logic in section Part I of the notebook template to iterate through each event file in `event_data` to 
process and create a new CSV file in Python
2. Make necessary edits to Part II of the notebook template to include Apache Cassandra `CREATE` and `INSERT` statements 
to load processed records into relevant tables in your data model
3. Test by running `SELECT` statements after running the queries on your database
