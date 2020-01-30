from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, TimestampType, StructField, StructType, StringType
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
        print("Status: Connecting to S3...")
        s3_file_str = "s3a://github-analysis-project/data-file/" + file_name + ".csv"

        if (file_name == 'commits'):
            res = self.spark.read.load(s3_file_str, format="csv", header=False, sep=',', schema=self.get_commits_schema())
            print("Status: COMPLETE")
            return res
        elif (file_name == 'users'):
            res = self.spark.read.load(s3_file_str, format="csv", header=False, sep=',', schema=self.get_users_schema())
            print("Status: COMPLETE")
            return res

    def get_commits_schema(self):
        commit_schema = StructType([
            StructField("commit_id", IntegerType(), True),
            StructField("sha", StringType(), True),
            StructField("author_id", IntegerType(), True),
            StructField("committer_id", IntegerType(), True),
            StructField("project_id", IntegerType(), True),
            StructField("created_at", TimestampType(), True)
        ])
        return commit_schema

    def get_users_schema(self):
            user_schema = StructType([
            	StructField("id", 	IntegerType(),True),	 	#_C0
            	StructField("login",    StringType(),True), 		#_c1
            	StructField("company",  StringType(),True), 		#_c2
            	StructField("created_at", TimestampType(),True),	#_c3
            	StructField("type", 	StringType(),True),		#_c4
            	StructField("fake", 	IntegerType(),True), 		#_c5
            	StructField("deleted", 	IntegerType(),True),		#_c6
            	StructField("long", 	DecimalType(),True),		#_c7
            	StructField("lat", 	DecimalType(),True),		#_c8
            	StructField("country_code", StringType(),True),		#_c9
            	StructField("country", 	StringType(), True),		#_c10
            	StructField("state",  StringType(),   True),		#_c11
            	StructField("city",  StringType(),    True),             #_c12
    	   ])
           return user_schema
