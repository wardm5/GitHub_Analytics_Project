# Class to connect to PostgreSQL database
class Connector():
    # Constructor for class, sets the database, url for connection, and properties needed for connection
    def __init__(self):
        self.database = 'test'
        self.url = "jdbc:postgresql://database-1.cu6pvppk2zw2.us-west-2.rds.amazonaws.com:5432/" + self.database
        self.properties = {"user": "postgres","password": "Trotsky1","driver": "org.postgresql.Driver"}

    # Write method for connection, will write to the PostgreSQL database
    def write(self, data_frame, mode, name):
        print("Status: writing to database... ", name)
        data_frame.write.jdbc(url=self.url, table=name, mode=mode, properties=self.properties)
        print("Status: COMPLETE")
