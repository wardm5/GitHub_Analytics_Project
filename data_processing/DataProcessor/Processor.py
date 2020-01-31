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
        self.commits = None         # table to store commit info
        self.users = None           # table to store user info
        self.projects = None        # table to store project info
        self.started = False        # flag to see if setup
        # IDEA, make a map of name to dataframe for keeping track of tables...

    # Method to write tables to PostgreSQL database
    def write_to_postgres(self):
        if (self.started == False):
            return
        self.postgres_connector.write(commits, 'overwrite')
        self.postgres_connector.write(users, 'overwrite')
        self.postgres_connector.write(projects, 'overwrite')

    # Method to read from S3 database
    def read_from_tables(self):
        self.users = self.s3_reader.read('users')
        self.projects = self.s3_reader.read('projects')
        self.commits = self.s3_reader.read('commits')
        self.users = self.users.filter(self.users.id.isNotNull())
        self.started = True

    # Method to count all table rows
    def count_table_rows(self):
        if (self.started == False):
            return
        print("Status: counting rows of tables")
        print("commit count: " , self.commits.count())
        print("users count: " , self.users.count())
        print("projects count: " , self.projects.count())

    # Method to get table stored in class
    def get_table(self, name):
        if (self.started == False):
            return
        elif (name == "commits"):
            return self.commits
        elif (name == "users"):
            return self.users
        elif (name == "projects"):
            return self.projects
        else:
            "Incorrect table selected"

    # get_table_names(self):

    # Method to preprocess tables by removing unneeded columns
    def preprocess_tables(self):
        if (self.started == False):
            return
        print("Status: dropping columns: commits")
        columns_to_drop_commits = ['sha', 'author_id']
        self.commits = self.commits.drop(*columns_to_drop_commits)

        print("Status: dropping columns: users")
        columns_to_drop_users = ['company', 'type', 'fake', 'long', 'lat']
        self.users = self.users.drop(*columns_to_drop_users)

        print("Status: dropping columns: projects")
        columns_to_drop_projects = ['forked_from', 'deleted']
        self.projects = self.projects.drop(*columns_to_drop_projects)

    # Method to show one specific table
    def show_table(self, name):
        if (name == 'users'):
            self.users.show()
        elif (name == 'projects'):
            self.projects.show()
        elif (name == 'commits'):
            self.commits.show()
        else:
            print("Incorrect table name, please try again")

# conn = Connector()
# conn.write(df_users, 'overwrite')
