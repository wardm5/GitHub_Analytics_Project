from pyspark.sql.types import IntegerType, TimestampType, StructField, StructType, StringType, BooleanType, DecimalType
class Schemas():
    def get_commits_schema(self):
        commit_schema = StructType([
            StructField("commit_id", IntegerType(), False),
            StructField("sha", StringType(), True),
            StructField("author_id", IntegerType(), True),
            StructField("committer_id", IntegerType(), True),
            StructField("project_id", IntegerType(), True),
            StructField("created_at", TimestampType(), True)
        ])
        return commit_schema

    def get_users_schema(self):
        user_schema = StructType([
            StructField("id", 	IntegerType(),False),	 	       #_C0
            StructField("login",    StringType(),True), 		   #_c1
            StructField("company",  StringType(),True), 		   #_c2
            StructField("created_at", TimestampType(),True),       #_c3
            StructField("type", 	StringType(),True),            #_c4
            StructField("fake", 	IntegerType(),True),           #_c5
            StructField("deleted", 	IntegerType(),True),		   #_c6
            StructField("long", 	DecimalType(),True),		   #_c7
            StructField("lat", 	DecimalType(),True),		       #_c8
            StructField("country_code", StringType(),True),		   #_c9
            StructField("country", 	StringType(), True),		   #_c10
            StructField("state",  StringType(),   True),		   #_c11
            StructField("city",  StringType(),    True),           #_c12
        ])
        return user_schema

    def get_projects_schema(self):
        project_schema = StructType([
            StructField("id", 	        IntegerType(),False),	 	#_C0
            StructField("url", 	        StringType(),True),	 	    #_C1
            StructField("owner_id", 	IntegerType(),True),	 	#_C2
            StructField("name", 	    StringType(),True),	 	    #_C3
            StructField("description", 	StringType(),True),	 	    #_C4
            StructField("language", 	StringType(),True),	 	    #_C5
            StructField("created_at", 	TimestampType(),True),	 	#_C6
            StructField("forked_from", 	IntegerType(),True),	 	#_C7
            StructField("deleted", 	    BooleanType(),True),	 	#_C8
            StructField("updated_at", 	TimestampType(),True),	 	#_C9
        ])
        return project_schema

    def get_repo_label_schema(self):
        repo_label_schema = StructType([
            StructField("id", 	        IntegerType(),False),	 	#_C0
            StructField("repo_id", 	    IntegerType(),True),	 	#_C1
            StructField("name", 	    StringType(),True),	 	    #_C2
        ])
        return repo_label_schema
