from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .getOrCreate()
# csvDf = spark.read.csv("s3a://path/to/files/*.csv")
df = spark.read.csv("s3a://github-analysis-project/data-file/followers.csv",header=True,sep=",")

print(df.collect())
