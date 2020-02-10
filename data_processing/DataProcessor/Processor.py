from pyspark.sql import SparkSession
from pyspark.sql import DataFrameWriter, DataFrameReader
from pyspark.sql import SQLContext
from PostgresConnector.Connector import *
from S3Reader.Reader import *
from pyspark.sql import functions as F
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.functions import percent_rank
import time, datetime

class Processor():
    # Constructor for processor, will setup S3 Reader, and PostgreSQL Connector
    def __init__(self, bucket_path, dns, port, db_user, password):
        self.postgres_connector = Connector(dns, port, db_user, password)   # writes tables to PostgreSQL
        self.s3_reader = Reader(bucket_path)               # reads tables from S3
        self.table_map = {}
        self.timer = None
        self.started = False        # flag to see tables have been read from database

    # Method to write tables to PostgreSQL database
    def write_to_postgres(self):
        if (self.started == False):
            return
        self.postgres_connector.write(commits, 'overwrite', 'commits')
        self.postgres_connector.write(users, 'overwrite', 'users')
        self.postgres_connector.write(projects, 'overwrite', 'projects')

    # Method to write to PostgreSQL database for specific table
    def write_specific_table_to_postgres(self, table_name):
        try:
            df = self.table_map.get(table_name)
            self.postgres_connector.write(df, 'overwrite', table_name, self.timer)
            self.timer = None
        except KeyError:
            print("Status: FAILURE - did not write to PostgreSQL database. ")
            print("Make sure you spelt your table name correctly")
            print(KeyError)
            pass

    # Method allows a user to search for their own file
    #---- currently assumes data is in csv, could change going forward
    def read_from_specific_table(self, file_name):
        user_generated_file = self.s3_reader.read(file_name)
        self.table_map[file_name] = user_generated_file

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
    def get_table(self, table_name):
        if (self.started == False):
            return
        if (self.table_map.get(table_name) != None):
            return self.table_map.get(table_name)
        else:
            "Incorrect table selected"

    # Method to print and return the table names stored in the map
    def get_table_names(self):
        print("Tables include:  ", self.table_map.keys())
        return self.table_map.keys()

    # Method to delete table in table_map
    def delete_table(self, table_name):
        if (self.table_map.get(table_name) != None):
            self.table_map.pop()
        else:
            print("Error: no table to delete, please try a different table name")

    # Simple method to start running a time stamp
    def start_timestamp(self):
        self.timer = time.time()
        return self.timer

    # Method to end timestamp, records to log
    def end_timestamp(self, method):
        with open("/home/ubuntu/data_processing/log/log.txt", "a") as myfile:
            myfile.write('TIME COMPLETED: ' + str(datetime.datetime.now()) + ',RUN TIME: ' + str(int(time.time() - self.timer)) + ' seconds, METHOD: ' + method + '\n')

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
        start = self.start_timestamp()
        if (self.started == None):
            return
        print("Status: Getting commit counts per project")
        projects = self.table_map['projects'].alias('projects')
        prod_lang = self.table_map['project_languages'].alias('prod_lang')
        inner_join = projects.join(prod_lang, projects.id == prod_lang.project_id) \
                        .select(projects.owner_id.alias('author'), projects.url, \
                        projects.name.alias('project_name'), projects.id.alias('product_id'), \
                        prod_lang.language, prod_lang.bytes, projects.forked_from, \
                        projects.deleted, projects.updated_at, projects.description) \
                        .where(projects.updated_at == prod_lang.created_at)
        inner_join = inner_join.orderBy('owner_id', 'id', inner_join['bytes'].desc())
        inner_join = inner_join.alias('inner_join')
        users = self.table_map['users'].alias('users')
        inner_join = inner_join.join(users, users.id == inner_join.author) \
                        .select(users.login, users.id, users.city, inner_join.url, \
                        inner_join.project_name, inner_join.language, inner_join.bytes, \
                        inner_join.deleted, inner_join.updated_at, inner_join.description) \
                        .where(users.country_code == "us")
        inner_join = inner_join.orderBy(inner_join.login, )
        self.table_map['pie_chart_data'] = inner_join
        self.end_timestamp('create_pie_chart_data')

    # Creates table for language usesage - might not need, could use info from internet on top languages
    def calculate_top_languages(self):
        self.start_timestamp()
        prod_lang = self.table_map['project_languages'].alias('prod_lang')
        # print(prod_lang.count())  138205530 -> 50 rows
        prod_lang = prod_lang.groupBy(prod_lang.language).agg(F.sum(prod_lang.bytes).alias('sum'))
        prod_lang = prod_lang.orderBy(prod_lang.sum.desc(), prod_lang.language)
        prod_lang = prod_lang.select(prod_lang.language, (prod_lang.sum / 1073741824).alias('sum')).limit(50)
        # prod_lang.show()
        self.table_map['languages_data'] = prod_lang
        end_timestamp('calculate_top_languages')

    # Creates table that ranks cities
    def calculate_top_cities(self):
        self.start_timestamp()
        users = self.table_map['users'].alias('users')
        users = users.groupBy(users.city, users.country_code).agg(F.count(users.id).alias('count_of_cities')).where(users.country_code == "us")
        users = users.orderBy(users.count_of_cities.desc())
        users = users.limit(200)
        self.table_map['cities_data'] = users
        self.end_timestamp('calculate_top_cities')

    # Creates table that would rank candidates and return percentile
    def calculate_commits(self):
        self.start_timestamp()
        users = self.table_map['users'].alias('users')
        users = users.select(users.login, users.id, users.city).where(users.country_code == "us")
        commits = self.table_map['commits'].alias('commits')
        inner_join = users.join(commits, users.id == commits.committer_id) \
                .groupBy(users.login, users.city).agg(F.count(commits.id).alias('count_of_commits'))
        inner_join = inner_join.orderBy(inner_join.count_of_commits.asc())
        self.table_map['commits_users_data'] = inner_join
        self.end_timestamp('commits_users_data')

    # Method to help calculate percentile of user compared to other users
    def calculate_project_sum(self):
        self.start_timestamp()
        projects_sum = self.table_map['pie_chart_data'].alias('pie_chart_data')
        projects_sum = projects_sum.groupBy(projects_sum.login).agg(F.sum(projects_sum.bytes).alias('sum'))
        self.table_map['projects_sum'] = projects_sum
        self.end_timestamp('projects_sum')
