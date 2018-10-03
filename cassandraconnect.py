import csv

from cassandra import ConsistencyLevel
from cassandra.policies import DCAwareRoundRobinPolicy
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster, BatchStatement
from cassandra.query import SimpleStatement

cluster = Cluster(
    contact_points=[
        "18.235.69.113", "54.91.137.215", "54.159.13.121" # AWS_VPC_US_EAST_1 (Amazon Web Services (VPC))
    ],
    load_balancing_policy=DCAwareRoundRobinPolicy(local_dc='AWS_VPC_US_EAST_1'), # your local data centre
    port=9042,
        auth_provider = PlainTextAuthProvider(username='iccassandra', password='242fcd6b82d96fcfa9b61f289e907caa')
)
session = cluster.connect()

def convert_csv(fname):

    results = []

    try:
        with open(fname) as csvfile:
            reader = csv.reader(csvfile, quoting=csv.QUOTE_ALL)
            for row in reader:
                results.append(row)

            #for i in range(len(results)):
            #    for j in range(len(results[i])):
            #        print(results[i][j] + " ")    
            #    print("####New entry####")
    except IOError:
        print("Invalid file")

    return results

def createKeyspace(keyspaceName):
    print("Creating KEYSPACE IF NOT EXISTS")
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS %s
        WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': '3' }
        """ % keyspaceName)
    session.set_keyspace(keyspaceName)

def createTable():
    print("Creating TABLE IF NOT EXISTS")
    session.execute("""
        CREATE TABLE IF NOT EXISTS movietable (
            title text,
            release_year text,
            genre text,
            director text,
            PRIMARY KEY (title, director)
        )
        """)

def insertData(data):
    print("Inserting Data")

    if len(data) == 0:
        print("Nope mate")

    prepared = session.prepare("""
            INSERT INTO movietable (title, release_year, genre, director)
            VALUES (?, ?, ?, ?)
            """)

    batch = BatchStatement()
    
    for i in range(1, len(data)):
        batch.add(prepared, (data[i][0], data[i][1], data[i][2], data[i][3]))

    session.execute(batch)

def fetchTableData():
    rows = session.execute('SELECT * FROM movietable')
    for row in rows:
        print(row)

if __name__ == '__main__':
    print("ok")
    data = convert_csv("movies.csv")

    createKeyspace("moviekeyspace")
    createTable()
    insertData(data)
    fetchTableData()

    cluster.shutdown()