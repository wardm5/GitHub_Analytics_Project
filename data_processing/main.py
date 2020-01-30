# spark-submit --master spark://ec2-44-231-60-234.us-west-2.compute.amazonaws.com:7077 testing.py
# from pyspark.sql import SparkSession
# from pyspark.sql import DataFrameWriter, DataFrameReader
# from pyspark.sql import SQLContext
# from PostgresConnector.Connector import *
# from S3Reader.Reader import *
from DataProcessor.Processor import *
# from pyspark.sql import functions as F
# from pyspark.sql.functions import *

# def count_table_rows():
#     print("Status: counting rows of tables")
#     print("commit count: " , commits.count())
#     print("users count: " , users.count())
#     print("projects count: " , projects.count())

# reader = Reader()

# commits = reader.read("commits")
# users = reader.read("users")
# projects = reader.read("projects")

# count_table_rows()

program = Processor()
program.read_from_tables()

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
