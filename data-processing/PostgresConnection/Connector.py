class Connector():
    def __init__():
        self.table = 'test'
        self.url = "jdbc:postgresql://database-1.cu6pvppk2zw2.us-west-2.rds.amazonaws.com:5432/" + self.table
        self.properties = {"user": "postgres","password": "Trotsky1","driver": "org.postgresql.Driver"}

    def write(data_frame, mode):
        df.write.jdbc(url=self.url, table="test_result", mode=mode, properties=self.properties)


    # table = 'test'
    # mode = "overwrite"
    # url = "jdbc:postgresql://database-1.cu6pvppk2zw2.us-west-2.rds.amazonaws.com:5432/" + table
    # df.write.jdbc(url=url, table="test_result", mode=mode, properties=self.properties)
    # spark.stop()
