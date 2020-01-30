from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, TimestampType, StructField, StructType, StringType
from Schemas import *
# Class to connect to PostgreSQL database
class Reader():
    # Constructor for class, sets the database, url for connection, and properties needed for connection
    def __init__(self):
        self.spark = spark = SparkSession \
                        .builder \
                        .appName("Python Spark SQL basic example") \
                        .getOrCreate()

    # Write method for connection, will write to the PostgreSQL database
    # Inputs:
    def read(self, file_name):
        schemas = Schemas()
        print("Status: Connecting to S3... " + file_name)
        s3_file_str = "s3a://github-analysis-project/data-file/" + file_name + ".csv"
        if (file_name == 'commits'):
            res = self.spark.read.load(s3_file_str, format="csv", header=False, sep=',', schema=schemas.get_commits_schema())
            print("Status: COMPLETE")
            return res
        elif (file_name == 'users'):
            res = self.spark.read.load(s3_file_str, format="csv", header=False, sep=',', schema=schemas.get_users_schema())
            print("Status: COMPLETE")
            return res
        elif (file_name == 'projects'):
            res = self.spark.read.load(s3_file_str, format="csv", header=False, sep=',', schema=schemas.get_projects_schema())
            print("Status: COMPLETE")
            return res
