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

    # Method to write to PostgreSQL database for specific table   **** pending testing
    def write_specific_table_to_postgres(self, name):
        if (self.dic['name'] != None):
            self.postgres_connector.write(name, 'overwrite')
        else:
            print("Error: No table by that name. Please re-try.")

    # Method to read from S3 database  **** pending testing****
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

    # Method to add specific table to dictionary (should be used for outside tables)
    def add_table(self, data_frame, name):
        self.dic['name'] = data_frame

    # Creates PERCENTILE TABLE for final project
    def create_percentile_table(self):
        if (self.started == None):
            return
        print("Status: Getting commit counts per user")
        commits = self.dic['commits'].groupBy('committer_id').agg(F.count('commit_id'))
        print("Status: joining users and commits tables")
        commits = self.dic['commits'].alias('commits')
        users = self.dic['users'].alias('users')
        inner_join = commits.join(users, commits.committer_id == users.id).select(users["login"],commits["*"])
        print("inner join table count: " , inner_join.count())
        inner_join.show()
        self.dic['percentiles'] = inner_join




# conn = Connector()
# conn.write(df_users, 'overwrite')
