from cassandra.cluster import Cluster
from cassandra.query import BatchStatement
from cassandra.query import ConsistencyLevel
import csv
import datetime
import gzip
import codecs
import sys
import os


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
