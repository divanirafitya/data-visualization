'''
==============================================================================

Nama: Divani Rafitya

This program is designed to automate the transformation and loading of data from 
PostgreSQL to Elasticsearch using Apache Airflow. The dataset used is the 
Customer Shopping Trends Dataset from Kaggle.
==============================================================================
'''

# import libraries
import pandas as pd
import numpy as np
import psycopg2 as db
import datetime as dt
from elasticsearch import Elasticsearch
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

# create function queryPostgresql
def queryPostgresql():
    '''
    Function to automate getting data from a table in PostgreSQL database.
    '''
    conn_string="dbname='airflow' host='postgres' user='airflow' password='airflow'"
    conn = db.connect(conn_string)
    df = pd.read_sql("select * from table_m3",conn)
    df.to_csv('/opt/airflow/dags/data_raw.csv',index=False)
    print("------Data Saved------")

# create function to clean raw data
def cleanedData():
    '''
    Function to automate the data cleaning, including standardize column name, 
    handling duplicates, and handling missing value.
    '''
    # load data
    df = pd.read_csv('/opt/airflow/dags/data_raw.csv')
    # handling duplicates
    df = df.drop_duplicates()
    # standardize column name
    df.columns = df.columns.str.lower().str.strip().str.replace(' ','_')
    df.columns = df.columns.replace('(','').replace(')','')
    # handling missing value with mode for categorical
    categorical = df.select_dtypes(include=object).columns.tolist()
    for cat in categorical:
        df[cat].fillna(df[cat].mode()[0], inplace=True)
    # handling missing value with mean for numerical
    numerical = df.select_dtypes(include=np.number).columns.tolist()
    for num in numerical:
        df[num].fillna(df[num].mean(), inplace=True)
    df.to_csv('/opt/airflow/dags/data_clean.csv')
    print("------Data Saved------")

# create function insertElasticsearch
def insertElasticsearch():
    '''
    Function to insert cleaned data into elasticsearch.
    '''
    es = Elasticsearch('http://elasticsearch:9200') 
    df = pd.read_csv('/opt/airflow/dags/data_clean.csv')
    for i,r in df.iterrows():
        doc = r.to_json()
        res = es.index(index="shoppingtrends",doc_type="doc",body=doc)
        print(res)	

# define default_args
default_args = {
    'owner': 'divani',
    'start_date': dt.datetime(2024, 7, 18),
    'retries': 5,
    'retry_delay': dt.timedelta(minutes=1)}

# define DAG
with DAG('ShoppingTrendsDAG',
    default_args=default_args,
    schedule_interval='30 6 * * *', #every 6.30 in the morning
    ) as dag:
    # starting
    start = BashOperator(task_id='Starting',
                         bash_command='echo "I am reading the CSV now..."')
    # get data from PostgreSQL
    getData = PythonOperator(task_id='QueryPostgreSQL',
                             python_callable=queryPostgresql)
    # data cleaning in python
    cleanData = PythonOperator(task_id='CleaningData',
                               python_callable=cleanedData)
    # insert insert cleaned data into elasticsearch
    insertData = PythonOperator(task_id='InsertElasticSearch',
                                python_callable=insertElasticsearch)

# define task dependencies
start >> getData >> cleanData >> insertData