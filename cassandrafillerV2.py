from cassandra.cluster import Cluster
from cassandra.query import BatchStatement
from cassandra.query import ConsistencyLevel
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
    stationReference = row["stationReference"]
    meta = {"parameter": row["parameter"], "qualifier": row["qualifier"]}
    values = row["value"].split("|")
    if len(values) > 1:
        meta.update({"state": "bad"})
    else:
        meta.update({"state": "good"})
    value = float(values[0])
    return (date, dateTime, stationReference, meta, value)


def cassandraConnection(clusterHostname):
    print("Connecting to: " + clusterHostname)
    cluster = Cluster([clusterHostname])
    cluster.connect()
    session = cluster.connect()
    print("Connected")
    session.execute("CREATE KEYSPACE IF NOT EXISTS dev WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 2}")
    session.execute("CREATE TABLE IF NOT EXISTS dev.measures ( date text, timestamp timestamp, stationReference text, meta map<text,text>, value double, PRIMARY KEY ((date), stationReference, timestamp))")
    return (cluster, session)

def putInCassandra(s3file, clusterHostname):
    (cluster, session) = cassandraConnection(clusterHostname)
    prepared = session.prepare("INSERT INTO dev.measures (date, timestamp, stationReference, meta, value) VALUES (?, ?, ?, ?, ?)")
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
            batch.add(prepared, createRow(row))
            count += 1
        session.execute(batch)
        batch.clear()
    print("Done")

#

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
    'dms-cassandraV2', default_args=default_args, schedule_interval='@daily')

s3ready = S3KeySensor( task_id='s3_file',
   poke_interval=0,
   timeout=15,
   soft_fail=False,
   bucket_key='s3://dms-deploy/flood-monitoring/archive/readings-full-{{ yesterday_ds }}.csv.gz',
   bucket_name=None,
   s3_conn_id=Variable.get("s3_connection"),
   dag=dag)


def doDatafile(cassandra, credentials, **kwargs):
    yesterday_ds = kwargs['yesterday_ds']

    filename = "readings-full-" + yesterday_ds + ".csv.gz"
    s3 = boto3.resource("s3",
      aws_access_key_id=credentials['aws_access_key_id'],
      aws_secret_access_key=credentials['aws_secret_access_key'])
    print("connecting to: " + cassandra)
    clusterHostname = cassandra
    try:
        print("Downloading File")
        s3.Bucket("dms-deploy").download_file("flood-monitoring/archive/" + filename, filename)
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            print("object doesn't exist")
        else:
            raise
    putInCassandra(filename, clusterHostname)
    os.remove(filename)
    return


connection = BaseHook.get_connection("s3_conn")
extra = connection.extra
parsed_extra = json.loads(extra)
cassandra = Variable.get("cassandrahost")
putIn = PythonOperator( task_id='put_in_cassandra',
        python_callable=doDatafile,
        provide_context=True,
        op_kwargs={'cassandra': cassandra, 'credentials': parsed_extra},
        dag=dag)

putIn.set_upstream(s3ready)
