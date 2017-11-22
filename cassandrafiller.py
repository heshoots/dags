from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.sensors import S3KeySensor
import sys
import os
from airflow.models import Variable
sys.path.append('/home/max/airflow/dags/cassandra-uploader/')
import datetime
from datetime import timedelta
from upload import putInCassandra
import boto3
import botocore

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.datetime(2017, 11, 6),
    'email': ['airflow@airflow.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'dms-cassandra', default_args=default_args, schedule_interval='@daily')

s3ready = S3KeySensor( task_id='s3_file',
   poke_interval=0,
   timeout=15,
   soft_fail=False,
   bucket_key='s3://dms-deploy/flood-monitoring/archive/readings-full-{{ ds }}.csv.gz',
   bucket_name=None,
   s3_conn_id=Variable.get("s3_connection"),
   dag=dag)


def doDatafile(**kwargs):
    execution_date = kwargs['execution_date']
    formatted = execution_date.strftime("%Y-%m-%d")

    filename = "readings-full-" + formatted + ".csv.gz"
    s3 = boto3.resource("s3")
    clusterHostname = "localhost"
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

putIn = PythonOperator( task_id='put_in_cassandra',
        python_callable=doDatafile,
        provide_context=True,
        dag=dag)

putIn.set_upstream(s3ready)
