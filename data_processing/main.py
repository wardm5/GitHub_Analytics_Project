# spark-submit --master spark://ec2-44-231-60-234.us-west-2.compute.amazonaws.com:7077 testing.py
from pyspark.sql import SparkSession
from pyspark.sql import DataFrameWriter, DataFrameReader
from pyspark.sql import SQLContext
from PostgresConnector.Connector import *
from S3Reader.Reader import *

reader = Reader()
df_users = reader.read("users")

df_users.printSchema()
df_users=df_users.drop(df_users.created_at)
df_res=df_users.groupby(df_users.user_id).count()



# print(df.count())

conn = Connector()
conn.write(df, 'overwrite')
