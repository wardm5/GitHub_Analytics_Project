from pyspark.sql import SparkSession

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
        print("Status: COMPLETE")
        return self.spark.read.csv(s3_file_str,header=False,sep=",")
