# spark-submit --master spark://ec2-44-231-60-234.us-west-2.compute.amazonaws.com:7077 testing.py
from pyspark.sql import SparkSession
from pyspark.sql import DataFrameWriter, DataFrameReader
from pyspark.sql import SQLContext
from PostgresConnector.Connector import *
from S3Reader.Reader import *
from pyspark.sql import functions as F

reader = Reader()

def



# df_users = reader.read("users")
df_commits = reader.read("commits")
df_users = reader.read("users")
df_commits.printSchema()
df_commits=df_commits.drop()  # drop column 4...
df_res = df_commits.groupBy('author_id').agg(F.count('c_id'))
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
