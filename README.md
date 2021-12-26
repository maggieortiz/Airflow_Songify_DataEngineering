# Airflow Songify DataEngineering

This repository holds loading information from 2 json files into a structured dimension and fact tables for song information. This is monitored with Airflow with connections to a AWS redshift storage DB. It also uses an S3 provided by udacity. 

The core DAG is on dags/udac_example_dag.py 

The operators are in plugins/operators/... 
  - data_quality - checks that there are no null values 
  - load dimension - loads dimenion tables 
  - load facts - loads fact tables 
  - stage_redshift - connects to AWS redshift 

Other files: 
- create_tables.sql - create table statements to be loaded in AWS with the query editor, or used in the create_table operator in airflow. 
- sql_queries.py - insert statements for tables 
