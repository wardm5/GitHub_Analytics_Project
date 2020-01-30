from pyspark.sql.types import IntegerType, TimestampType, StructField, StructType, StringType
class Schemas():
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

    # def get_project_schema(self):
    #         project_schema = StructType([
    #         	StructField("id", 	IntegerType(),True),	 	#_C0
    # 	   ])
    #        return project_schema
