# spark-submit --master spark://ec2-44-231-60-234.us-west-2.compute.amazonaws.com:7077 testing.py
from pyspark.sql import SparkSession
from pyspark.sql import DataFrameWriter, DataFrameReader
from pyspark.sql import SQLContext
from PostgresConnector.Connector import *
from S3Reader.Reader import *
from pyspark.sql import functions as F

reader = Reader()


df_commits = reader.read("commits")
df_users = reader.read("users")
columns_to_drop_commits = ['sha', 'project_id', 'author_id']
df_commits = df_commits.drop(*columns_to_drop)
df_commits.printSchema()

df_res = df_commits.groupBy('committer_id').agg(F.count('commit_id'))
# df_res=df_commits.groupby(_c3).count()
# print(df_users.head())
df_res.show()



# df_users.printSchema()
# df_users=df_users.drop(_c4)  # drop column 4...
# df_res=df_users.groupby(_c1).count()
# print(df_users.head())
# df_res.show()

# print(df.count())

# conn = Connector()
# conn.write(df_users, 'overwrite')
