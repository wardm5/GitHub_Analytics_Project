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

    def write_to_postgres(self):
        if (self.started == False):
            return
        self.postgres_connector.write(commits, 'overwrite')
        self.postgres_connector.write(users, 'overwrite')
        self.postgres_connector.write(projects, 'overwrite')

    def read_from_tables(self):
        self.commits = self.s3_reader.read('commits')
        self.users = self.s3_reader.read('users')
        self.projects = self.s3_reader.read('projects')
        self.started = True

    def count_table_rows(self):
        if (self.started == False):
            return
        print("Status: counting rows of tables")
        print("commit count: " , self.commits.count())
        print("users count: " , self.users.count())
        print("projects count: " , self.projects.count())

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

    def preprocess_tables(self):
        if (self.started == False):
            return
        print("Status: dropping columns: commits")
        columns_to_drop_commits = ['sha', 'author_id']
        self.commits.drop(*columns_to_drop_commits)

        print("Status: dropping columns: users")
        columns_to_drop_users = ['company', 'type', 'fake', 'long', 'lat']
        self.users.drop(*columns_to_drop_users)

        print("Status: dropping columns: projects")
        columns_to_drop_projects = ['forked_from', 'deleted']
        self.projects.drop(*columns_to_drop_projects)

    def show_table(self, name):
        if (name == 'users'):
            self.users.show()
        elif (name == 'projects'):
            self.projects.show()
        elif (name == 'commits'):
            self.commits.show()
        else:
            print("Incorrect table name, please try again")

# print("Status: dropping columns")
# columns_to_drop_commits = ['sha', 'author_id']
# commits = commits.drop(*columns_to_drop_commits)
#
# print("Status: counting commits")
# commits = commits.groupBy('committer_id').agg(F.count('commit_id')).orderBy('count(commit_id)', ascending=False)
# commits.printSchema()
# commits.show()

# print("Status: joining users and commits tables")
# commit = commits.alias('commits')
# user = users.alias('users')
# inner_join = commit.join(user, commit.committer_id == user.id).select(user["login"],commit["*"])
# print("inner join table count: " , inner_join.count())
# inner_join.show()

# conn = Connector()
# conn.write(df_users, 'overwrite')
