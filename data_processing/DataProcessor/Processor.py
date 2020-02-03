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
    def __init__(self, bucket_name):
        self.postgres_connector = Connector()   # writes tables to PostgreSQL
        self.s3_reader = Reader(bucket_name)               # reads tables from S3
        self.table_map = {}
        self.started = False        # flag to see tables have been read from database

    # Method to write tables to PostgreSQL database
    def write_to_postgres(self):
        if (self.started == False):
            return
        # self.postgres_connector.write(commits, 'overwrite', 'commits')
        # self.postgres_connector.write(users, 'overwrite', 'users')
        # self.postgres_connector.write(projects, 'overwrite', 'projects')

    # Method to write to PostgreSQL database for specific table
    def write_specific_table_to_postgres(self, name):
        try:
            df = self.table_map.get(name)
            print("Status: Writing to table ", name)
            self.postgres_connector.write(df, 'overwrite', name)
            print("Status: COMPLETE")
        except KeyError:
            print("Status: FAILURE - did not write to PostgreSQL database. ")
            print("Make sure you spelt your table name correctly")
            print(KeyError)
            pass

    # Method to read from S3 database  **** pending testing****
    def read_from_default_tables(self):
        # commit_comments = self.s3_reader.read('commit_comments')
        # self.table_map['commit_comments'] = commit_comments
        #*********************************************************************#
        commits = self.s3_reader.read('commits')
        self.table_map['commits'] = commits
        #*********************************************************************#
        # followers = self.s3_reader.read('followers')
        # self.table_map['followers'] = followers
        #*********************************************************************#
        project_languages = self.s3_reader.read('project_languages')
        self.table_map['project_languages'] = project_languages
        #*********************************************************************#
        projects = self.s3_reader.read('projects')
        self.table_map['projects'] = projects
        #*********************************************************************#
        # repo_labels = self.s3_reader.read('repo_labels')
        # self.table_map['repo_labels'] = repo_labels
        #*********************************************************************#
        users =  self.s3_reader.read('users')
        self.table_map['users'] = users
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

    # Method to print and return the table names stored in the map
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
        columns_to_drop_users = ['company', 'type', 'fake', 'deleted', 'long', 'lat']
        self.table_map['users'] = self.table_map['users'].drop(*columns_to_drop_users)

        print("Status: dropping columns: projects")
        columns_to_drop_projects = ['forked_commit_id']
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
    # Creates Pie Chart Graph
    def create_pie_chart_data(self):
        if (self.started == None):
            return
        # print(type(self.table_map['projects']))
        print("Status: Getting commit counts per project")
        projects = self.table_map['projects'].alias('projects')
        prod_lang = self.table_map['project_languages'].alias('prod_lang')
        inner_join = projects.join(prod_lang, projects.id == prod_lang.project_id) \
                        .select(projects.owner_id.alias('author'), projects.url, \
                        projects.name.alias('project_name') , projects.id.alias('product_id'), \
                        prod_lang.language, prod_lang.bytes, projects.forked_from, \
                        projects.deleted) \
                        .where(projects.updated_at == prod_lang.created_at)

        inner_join = inner_join.orderBy('owner_id', 'id', inner_join['bytes'].desc())
        print(type(inner_join))
        inner_join = inner_join.alias('inner_join')
        users = self.table_map['users'].alias('users')
        inner_join = inner_join.join(users, users.id == inner_join.author) \
                        .select(users.login, users.id, users.location, inner_join.url, \
                        inner_join.project_name, inner_join.language, inner_join.bytes, inner_join.deleted)
        inner_join = inner_join.orderBy(inner_join.login)
        inner_join.show()
        print(type(inner_join))
        self.table_map['pie_chart_data'] = inner_join
