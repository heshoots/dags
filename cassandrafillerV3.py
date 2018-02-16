from cassandra.cluster import Cluster
from cassandra.query import BatchStatement
from cassandra.query import ConsistencyLevel
from cassandra.auth import PlainTextAuthProvider
import csv
import gzip
import codecs
import sys
import os

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.base_hook import BaseHook
from airflow.operators.sensors import S3KeySensor
from airflow.models import Variable
import json
from airflow.models import Variable
import datetime
from datetime import timedelta
import boto3
import botocore

#

def createRow(row):
    date = row["date"]
    dateTime = datetime.datetime.strptime(row["dateTime"], '%Y-%m-%dT%H:%M:%SZ')
    # get last part of measure url
    measureReference = row["measure"].split("/")[-1]
    meta = {"parameter=" + row["parameter"], "qualifier=" + row["qualifier"]}
    values = row["value"].split("|")
    state = {"state=good"}
    # a value such as 1.0|0.0 implies something is wrong at the station
    if len(values) > 1:
        state = {"state=bad"}
    meta.update(state)
    # in all cases (including bad state, we take the first value given)
    value = float(values[0])
    return (date, dateTime, measureReference, meta, value)


def getSession(cassandraConnection):
    print("Connecting to: " + cassandraConnection.host)
    auth_provider = PlainTextAuthProvider(
            username=cassandraConnection.login, password=cassandraConnection.password)
    cluster = Cluster(["external-cassandra-0", "external-cassandra-1", "external-cassandra-2"], auth_provider=auth_provider)
    cluster.connect()
    return cluster.connect(cassandraConnection.schema)

def putInCassandra(s3file, cassandraConnection):
    session = getSession(cassandraConnection)
    measures_by_date = session.prepare("INSERT INTO measures_by_date (date, timestamp, measureReference, meta, value) VALUES (?, ?, ?, ?, ?)")
    measures_by_measurereference = session.prepare("INSERT INTO measures_by_measurereference (date, timestamp, measureReference, meta, value) VALUES (?, ?, ?, ?, ?)")
    batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
    batch_number = 10
    print("Opening File")
    count = 0
    with gzip.open(s3file, 'r') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            if (count >= batch_number):
                session.execute(batch)
                batch.clear()
                count = 0
            batch.add(measures_by_measurereference, createRow(row))
            batch.add(measures_by_date, createRow(row))
            count += 2
        session.execute(batch)
        batch.clear()
    print("Done")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.datetime(2015, 2, 2),
    'email': ['airflow@airflow.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'dms-cassandraV3', default_args=default_args, schedule_interval='@daily')

s3ready = S3KeySensor( task_id='s3_file',
   poke_interval=0,
   timeout=15,
   soft_fail=False,
   bucket_key='s3://dms-deploy/flood-monitoring/archive/readings-full-{{ yesterday_ds }}.csv.gz',
   bucket_name=None,
   s3_conn_id=Variable.get("s3_connection"),
   dag=dag)


def downloadDatafile(date, credentials):
    filename = "readings-full-" + date + ".csv.gz"
    s3 = boto3.resource("s3",
      aws_access_key_id=credentials['aws_access_key_id'],
      aws_secret_access_key=credentials['aws_secret_access_key'])
    try:
        print("Downloading File")
        s3.Bucket("dms-deploy").download_file("flood-monitoring/archive/" + filename, filename)
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            print("object doesn't exist")
        else:
            raise
    return filename

def insertDailyDump(cassandra, credentials, **kwargs):
    yesterday_ds = kwargs['yesterday_ds']
    filename = downloadDatafile(yesterday_ds, credentials)
    putInCassandra(filename, cassandra)
    os.remove(filename)

connection = BaseHook.get_connection("s3_conn")
extra = connection.extra
parsed_extra = json.loads(extra)
cassandra = BaseHook.get_connection("cassandra_connection")
putIn = PythonOperator( task_id='put_in_cassandra',
        python_callable=insertDailyDump,
        provide_context=True,
        op_kwargs={'cassandra': cassandra, 'credentials': parsed_extra},
        dag=dag)

putIn.set_upstream(s3ready)
