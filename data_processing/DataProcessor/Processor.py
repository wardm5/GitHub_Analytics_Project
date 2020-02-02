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
        self.table_map = {}
        self.started = False        # flag to see if setup

    # Method to write tables to PostgreSQL database
    def write_to_postgres(self):
        if (self.started == False):
            return
        self.postgres_connector.write(commits, 'overwrite', 'commits')
        self.postgres_connector.write(users, 'overwrite', 'users')
        self.postgres_connector.write(projects, 'overwrite', 'projects')

    # Method to write to PostgreSQL database for specific table
    def write_specific_table_to_postgres(self, name):
        try:
            df = self.table_map[name]
            self.postgres_connector.write(df, 'overwrite', name)
        except KeyError:
            print("Status: FAILURE - did not write to PostgreSQL database. ")
            pass

    # Method to read from S3 database  **** pending testing****
    def read_from_tables(self):
        users =  self.s3_reader.read('users')
        projects = self.s3_reader.read('projects')
        commits = self.s3_reader.read('commits')
        self.table_map['users'] = users
        self.table_map['projects'] = projects
        self.table_map['commits'] = commits
        self.started = True

    # Method to count all table rows
    def count_table_rows(self):
        if (self.started == False):
            return
        print("Status: counting rows of tables")
        print("commit count: " , self.table_map.get('users').count())
        print("commit count: " , self.table_map.get('projects').count())
        print("commit count: " , self.table_map.get('commits').count())

    # Method to get table stored in class
    def get_table(self, name):
        if (self.started == False):
            return
        if (self.table_map.get(name) != None):
            return self.table_map.get(name)
        else:
            "Incorrect table selected"


    def get_table_names(self):
        print("Tables include:  ", self.table_map.keys())
        return self.table_map.keys()

    # Method to preprocess tables by removing unneeded columns
    def preprocess_tables(self):
        if (self.started == False):
            return

        print("Status: dropping columns: commits")
        columns_to_drop_commits = ['sha', 'author_id']
        self.table_map['commits'] = self.table_map['commits'].drop(*columns_to_drop_commits)

        print("Status: dropping columns: users")
        columns_to_drop_users = ['company', 'type', 'fake', 'long', 'lat']
        self.table_map['users'] = self.table_map['users'].drop(*columns_to_drop_users)

        print("Status: dropping columns: projects")
        columns_to_drop_projects = ['forked_from', 'deleted']
        self.table_map['projects'] = self.table_map['projects'].drop(*columns_to_drop_projects)

    # Method to show one specific table
    def show_table(self, name):
        if (self.table_map.get(name) != None):
            self.table_map.get(name).show()
        else:
            print("Incorrect table name, please try again")

    # Method to add specific table to map (should be used for outside tables)
    def add_table(self, data_frame, name):
        self.table_map[name] = data_frame

    #**************************** Default Methods ****************************#
    # Creates Percentile Table for final project
    def create_default_table_1(self):
        if (self.started == None):
            return
        print("Status: Getting commit counts per user")
        commits = self.table_map['commits'].groupBy('committer_id').agg(F.count('commit_id'))
        commits = commits.orderBy(commits['count(commit_id)'].asc())
        commits.show()
        print(commits.count())
        commits_table = self.table_map['commits'].alias('commits_table')
        users_table = self.table_map['users'].alias('users_table')
        print("Status: joining users and commits tables")
        inner_join = commits.join(users_table, commits_table.committer_id == users_table.id).select(users_table["login"],commits_table["*"])
        # print("inner join table count: " , inner_join.count())
        inner_join.show()
        # self.table_map['percentiles'] = inner_join

    # Creates Language Percent for final project
    def create_bar_chart_of_langauge_table(self):
        if (self.started == None):
            return
        print("Status: Getting commit counts per project")
        projects = self.table_map['projects'].groupBy('owner_id', 'language').agg(count('id').alias('count')).select('owner_id', 'language', 'count')
        projects = projects.orderBy('owner_id', projects['count'].desc())
        projects_table = projects.alias('projects_table')
        users_table = self.table_map['users'].alias('users_table')
        print("Status: joining users and projects tables")
        inner_join = projects_table.join(users_table, projects_table.owner_id == users_table.id).select(users_table['*'],projects_table['*'])
        inner_join.show()
        self.table_map['default_2'] = inner_join

    def create_default_table_3(self):
        if (self.started == None):
            return
        print("Status: Getting commit counts per project")
        projects = self.table_map['projects'].groupBy('language', 'owner_id').agg(count('id').alias('count')).select('language', 'owner_id', 'count(id)')
        projects = projects.orderBy('language', projects['count'].desc())
        window = Window.partitionBy(projects['language']).orderBy(projects['count'].desc())
        projects = projects.select('*', percent_rank().over(window).alias('rank'))
        projects_table = projects.alias('projects_table')
        users_table = self.table_map['users'].alias('users_table')
        print("Status: joining users and projects tables")
        inner_join = projects_table.join(users_table, projects_table.owner_id == users_table.id).select(users_table["login"],projects_table['*'])
        inner_join = inner_join.orderBy('language', 'count')
        inner_join.show()
        # print(inner_join.count())
        self.table_map['default_3'] = inner_join

    def create_projects_language_filter_table(self):
        if (self.started == None):
            return
        print("Status: Getting commit counts per project")
        projects = self.table_map['projects'].groupBy('language', 'owner_id').agg(count('id').alias('count')).select('language', 'owner_id', 'count(id)')
        projects = projects.orderBy('language', projects['count'].desc())
        window = Window.partitionBy(projects['language']).orderBy(projects['count'].desc())
        projects = projects.select('*', percent_rank().over(window).alias('rank'))
        projects_table = projects.alias('projects_table')
        users_table = self.table_map['users'].alias('users_table')
        print("Status: joining users and projects tables")
        inner_join = projects_table.join(users_table, projects_table.owner_id == users_table.id).select(users_table["login"],projects_table['*'])
        inner_join = inner_join.orderBy('language', 'count')
        inner_join.show()
        # print(inner_join.count())
        self.table_map['default_4'] = inner_join
