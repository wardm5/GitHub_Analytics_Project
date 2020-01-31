from pyspark.sql import SparkSession
from pyspark.sql import DataFrameWriter, DataFrameReader
from pyspark.sql import SQLContext
from PostgresConnector.Connector import *
from S3Reader.Reader import *
from pyspark.sql import functions as F
from pyspark.sql.functions import *

class Processor():
    def __init__(self):
        self.postgres_connector = Connector()   # writes tables to PostgreSQL
        self.s3_reader = Reader()               # reads tables from S3
        self.dic = {}
        self.started = False        # flag to see if setup

    # Method to write tables to PostgreSQL database
    def write_to_postgres(self):
        if (self.started == False):
            return
        self.postgres_connector.write(commits, 'overwrite')
        self.postgres_connector.write(users, 'overwrite')
        self.postgres_connector.write(projects, 'overwrite')

    # Method to read from S3 database
    def read_from_tables(self):
        users =  self.s3_reader.read('users')
        projects = self.s3_reader.read('projects')
        commits = self.s3_reader.read('commits')
        self.dic['users'] = users
        self.dic['projects'] = projects
        self.dic['commits'] = commits
        self.started = True

    # Method to count all table rows
    def count_table_rows(self):
        if (self.started == False):
            return
        print("Status: counting rows of tables")
        print("commit count: " , self.dic.get('users').count())
        print("commit count: " , self.dic.get('projects').count())
        print("commit count: " , self.dic.get('commits').count())

    # Method to get table stored in class
    def get_table(self, name):
        if (self.started == False):
            return
        if (self.dic.get(name) != None):
            return self.dic.get(name)
        else:
            "Incorrect table selected"

    def get_table_names(self):
        print("Tables include:  ", self.dic.keys())
        return self.dic.keys()

    # Method to preprocess tables by removing unneeded columns
    def preprocess_tables(self):
        if (self.started == False):
            return

        print("Status: dropping columns: commits")
        columns_to_drop_commits = ['sha', 'author_id']
        self.dic['commits'] = self.dic['commits'].drop(*columns_to_drop_commits)

        print("Status: dropping columns: users")
        columns_to_drop_users = ['company', 'type', 'fake', 'long', 'lat']
        self.dic['users'] = self.dic['users'].drop(*columns_to_drop_users)

        print("Status: dropping columns: projects")
        columns_to_drop_projects = ['forked_from', 'deleted']
        self.dic['projects'] = self.dic['projects'].drop(*columns_to_drop_projects)

    # Method to show one specific table
    def show_table(self, name):
        if (self.dic.get(name) != None):
            self.dic.get(name).show()
        else:
            print("Incorrect table name, please try again")

# conn = Connector()
# conn.write(df_users, 'overwrite')
