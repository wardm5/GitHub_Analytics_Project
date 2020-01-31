from pyspark.sql import SparkSession
from pyspark.sql import DataFrameWriter, DataFrameReader
from pyspark.sql import SQLContext
from PostgresConnector.Connector import *
from S3Reader.Reader import *
from pyspark.sql import functions as F
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.functions import percent_rank

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
        self.postgres_connector.write(commits, 'overwrite', 'commits')
        self.postgres_connector.write(users, 'overwrite', 'users')
        self.postgres_connector.write(projects, 'overwrite', 'projects')

    # Method to write to PostgreSQL database for specific table   **** pending testing
    def write_specific_table_to_postgres(self, name):
        try:
            df = self.dic[name]
            self.postgres_connector.write(df, 'overwrite', name)
        except KeyError:
            pass

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
        self.dic[name] = data_frame

    # Creates Percentile Table for final project
    def create_default_table_1(self):
        if (self.started == None):
            return
        print("Status: Getting commit counts per user")
        commits = self.dic['commits'].groupBy('committer_id').agg(F.count('commit_id'))
        commits = commits.orderBy(commits['count(commit_id)'].asc())
        commits.show()
        print(commits.count())
        commits_table = self.dic['commits'].alias('commits_table')
        users_table = self.dic['users'].alias('users_table')
        print("Status: joining users and commits tables")
        inner_join = commits.join(users_table, commits_table.committer_id == users_table.id).select(users_table["login"],commits_table["*"])
        # print("inner join table count: " , inner_join.count())
        inner_join.show()
        # self.dic['percentiles'] = inner_join

    # Creates Language Percent for final project
    def create_default_table_2(self):
        if (self.started == None):
            return
        print("Status: Getting commit counts per project")
        projects = self.dic['projects'].groupBy('owner_id', 'language').agg(count('id')).select('owner_id', 'language', 'count(id)')
        projects = projects.orderBy('owner_id', projects['count(id)'].desc())
        projects_table = projects.alias('projects_table')
        users_table = self.dic['users'].alias('users_table')
        print("Status: joining users and commits tables")
        inner_join = projects_table.join(users_table, projects_table.owner_id == users_table.id).select(users_table['*'],projects_table['*'])
        inner_join.show()
        self.dic['default_2'] = inner_join

    def create_default_table_3(self):
        if (self.started == None):
            return
        print("Status: Getting commit counts per project")
        projects = self.dic['projects'].groupBy('language', 'owner_id').agg(count('id')).select('language', 'owner_id', 'count(id)')
        projects = projects.orderBy('language', projects['count(id)'].desc())
        window = Window.partitionBy(projects['language']).orderBy(projects['count(id)'].desc())
        projects = projects.select('*', percent_rank().over(window).alias('rank'))
        projects.show()
        projects_table = projects.alias('projects_table')
        users_table = self.dic['users'].alias('users_table')
        print("Status: joining users and commits tables")
        inner_join = projects_table.join(users_table, projects_table.owner_id == users_table.id).select(users_table["login"],projects_table['*'])
        inner_join = inner_join.orderBy('language', 'count(id)')
        inner_join.show()
        # print(inner_join.count())
        self.dic['default_3'] = inner_join
        # projects_table = projects.alias('projects_table')

        # users_table = self.dic['users'].alias('users_table')
# conn = Connector()
# conn.write(df_users, 'overwrite')
