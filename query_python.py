from influxdb_client_3 import InfluxDBClient3

client = InfluxDBClient3(
    host='http://localhost:8181',

    database='sensors'
)

# Execute the query and return an Arrow table
table = client.query(
    query="SELECT count(*) as c FROM cpu",
    language="sql"
)

print("\n#### View Schema information\n")
print(table.schema)

print("\n#### Use PyArrow to read the specified columns\n")
print(table.column('c'))

