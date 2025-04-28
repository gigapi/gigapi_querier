from flightsql import connect, FlightSQLClient

client = FlightSQLClient(host='localhost',port=8082,insecure=True,metadata={'bucket':'hep'})
conn = connect(client)
cursor = conn.cursor()
cursor.execute('SELECT 1, version()')
# print("columns:", cursor.description)
print("rows:", [r for r in cursor])
