# Class to connect to PostgreSQL database
class Connector():
    # Constructor for class, sets the database, url for connection, and properties needed for connection
    def __init__(self, dns, port, db_user, password):
        self.database = 'test'
        self.url = "jdbc:postgresql://" + dns + ":" + port + "/" + self.database
        self.properties = {"user": db_user, "password": password, "driver": "org.postgresql.Driver"}

    # Write method for connection, will write to the PostgreSQL database
    def write(self, data_frame, mode, name):
        print("Status: writing to database... ", name)
        data_frame.write.jdbc(url=self.url, table=name, mode=mode, properties=self.properties)
        print("Status: COMPLETE")
