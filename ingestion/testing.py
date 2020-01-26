# spark-submit --master spark://ec2-44-231-60-234.us-west-2.compute.amazonaws.com:7077 testing.py
from pyspark.sql import SparkSession

spark = SparkSession\
        .builder\
        .appName("Python Testing")\
        .getOrCreate()

df = spark.read.csv("s3a://github-analysis-project/data-file/followers.csv",header=True,sep=",")
print(df.count())
spark.stop()
