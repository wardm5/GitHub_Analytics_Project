from pyspark.sql import SparkSession
from pyspark.sql import DataFrameWriter, DataFrameReader
from pyspark.sql import SQLContext
from PostgresConnector.Connector import *
from S3Reader.Reader import *
from pyspark.sql import functions as F
from pyspark.sql.functions import *

class Processor():
    def __init__(self):
        self.postgres_connector = Connector()
        self.s3_reader = Reader()
        self.commits = None
        self.users = None
        self.projects = None
        self.started = False

    def read_from_tables(self):
        self.commits = self.s3_reader.read("commits")
        self.users = self.s3_reader.read("users")
        self.projects = self.s3_reader.read("projects")
        self.started = True

    def count_table_rows(self):
        if (self.started == False):
            return
        print("Status: counting rows of tables")
        # print("commit count: " , self.commits.count())
        # print("users count: " , self.users.count())
        print("projects count: " , self.projects.count())
