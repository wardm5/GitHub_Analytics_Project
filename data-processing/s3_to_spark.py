from pyspark.sql import SparkSession
spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .getOrCreate()
df = spark.read.csv("s3a://github-analysis-project/data-file/followers.csv",header=True,sep=",")

print(df.count())
