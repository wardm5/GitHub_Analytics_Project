# spark-submit --master spark://ec2-44-231-60-234.us-west-2.compute.amazonaws.com:7077 testing.py
from pyspark.sql import SparkSession
from pyspark.sql import DataFrameWriter, DataFrameReader
from pyspark.sql import SQLContext


spark = SparkSession\
        .builder\
        .appName("Python Testing")\
        .getOrCreate()

df = spark.read.csv("s3a://github-analysis-project/data-file/organization_members.csv",header=False,sep=",")
print(df.count())

def get_writer(data_frame):
    ''' gets writer '''
    return DataFrameWriter(data_frame)

url_connect = 'database-1.cu6pvppk2zw2.us-west-2.rds.amazonaws.com'
table = 'test'
mode = "overwrite"
url = "jdbc:postgresql://database-1.cu6pvppk2zw2.us-west-2.rds.amazonaws.com:5432/test"
properties = {"user": "postgres","password": "Trotsky1","driver": "org.postgresql.Driver"}
df.write.jdbc(url=url, table="test_result", mode=mode, properties=properties)
spark.stop()
