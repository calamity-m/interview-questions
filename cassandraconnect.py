import csv

# Import parts of the Cassandra Python Drivers that we need
from cassandra import ConsistencyLevel
from cassandra.policies import DCAwareRoundRobinPolicy
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster, BatchStatement
from cassandra.query import SimpleStatement

# Create the cluster object that we use to connect
cluster = Cluster(
    contact_points=[
        "18.235.69.113", "54.91.137.215", "54.159.13.121" # AWS_VPC_US_EAST_1 (Amazon Web Services (VPC))
    ],
    load_balancing_policy=DCAwareRoundRobinPolicy(local_dc='AWS_VPC_US_EAST_1'), # your local data centre
    port=9042,
        auth_provider = PlainTextAuthProvider(username='iccassandra', password='242fcd6b82d96fcfa9b61f289e907caa')
)

# Create a session and connect to the cluster
session = cluster.connect()

# Converts a CSV File at fname into an array
def convert_csv(fname):

    results = []

    # Attempt to read and convert the CSV File
    try:
        with open(fname) as csvfile:
            reader = csv.reader(csvfile, quoting=csv.QUOTE_ALL)
            for row in reader:
                results.append(row)
    
    # Catch any IO Errors
    except IOError:
        print("Invalid file")

    return results

# Initializes a keyspace if it does not already exist
def createKeyspace(keyspaceName):
    print("Creating KEYSPACE IF NOT EXISTS")

    # Calls the following CQL Query creating a keyspace
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS %s
        WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': '3' }
        """ % keyspaceName)
    
    # Set the keyspace of the session to our new or old keyspace
    session.set_keyspace(keyspaceName)

# Initializes a table if it does not already exist
def createTable():
    print("Creating TABLE IF NOT EXISTS")

    # Calls the following CQL Query creating a table
    session.execute("""
        CREATE TABLE IF NOT EXISTS movietable (
            title text,
            release_year text,
            genre text,
            director text,
            PRIMARY KEY (title, director)
        )
        """)

# Populate the table with data
def insertData(data):
    print("Inserting Data")

    # If we have no data, exit
    if len(data) == 0:
        print("No data, exiting data insertion")
        return

    # Create a modifiable CQL Query
    prepared = session.prepare("""
            INSERT INTO movietable (title, release_year, genre, director)
            VALUES (?, ?, ?, ?)
            """)

    # Create our batch statement
    batch = BatchStatement()
    
    # Populate the batch statement with data
    for i in range(1, len(data)):
        batch.add(prepared, (data[i][0], data[i][1], data[i][2], data[i][3]))

    # Calls the batch statement, inserting data into the table
    session.execute(batch)

# Display all data from the table
def fetchTableData():
    # For every row from the query print it to console
    rows = session.execute('SELECT * FROM movietable')
    for row in rows:
        print(row)

if __name__ == '__main__':
    # Convert our Data
    data = convert_csv("movies.csv")

    # Create our keyspace
    createKeyspace("moviekeyspace")

    # Create our table
    createTable()

    # Populate our table with converted data
    insertData(data)

    # Display our table data
    fetchTableData()

    # End session by shutting down
    cluster.shutdown()