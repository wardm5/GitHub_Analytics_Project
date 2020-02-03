from pyspark.sql.types import IntegerType, TimestampType, StructField, StructType, StringType, BooleanType, DecimalType
class Schemas():
    # Constructor that creates map to store schemas
    def __init__(self):
        self.schema_map = {}
        # adds default schemas to map
        self.add_schema_to_map('commits', self.get_commits_schema())
        self.add_schema_to_map('users', self.get_users_schema())
        self.add_schema_to_map('projects', self.get_projects_schema())

    # Method to add schema to schema map
    def add_schema_to_map(self, name, schema):
        self.schema_map[name] = schema

    # Method to get schema from map
    def get_schema(self, name):
        if (self.schema_map.get(name) != None):
            return self.schema_map.get(name)
        else:
            "Incorrect schema selected"

    #**************************** Default schemas ****************************#
    # Method to return the commits schema
    def get_commits_schema(self):
        schema = StructType([
            StructField("id", IntegerType(), False),
            StructField("sha", StringType(), True),
            StructField("author_id", IntegerType(), True),
            StructField("committer_id", IntegerType(), True),
            StructField("project_id", IntegerType(), True),
            StructField("created_at", TimestampType(), True)
        ])
        return schema

    # Method to return the commit_comments schema
    def get_commits_schema(self):
        schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("commit_id", IntegerType(), True),
            StructField("user_id", IntegerType(), True),
            StructField("body", StringType(), True),
            StructField("line", IntegerType(), True),
            StructField("position", IntegerType(), True),
            StructField("comment_id", IntegerType(), True),
            StructField("created_at", TimestampType(), True)
        ])
        return schema

    # Method to return followers schema
    def get_followers_schema(self):
        schema = StructType([
            StructField("user_id", IntegerType(), True),
            StructField("follower_id", IntegerType(), True),
            StructField("created_at", TimestampType(), True)
        ])
        return schema

    # Method to return the projects schema
    def get_projects_schema(self):
        schema = StructType([
            StructField("project_id", 	  IntegerType(),False),	 	#_C0
            StructField("language", 	  StringType(),True),	 	#_C1
            StructField("bytes", 	      IntegerType(),True),	 	#_C2
            StructField("created_at", 	  TimestampType(),True)     #_C3
        ])
        return schema

    # Method to return the projects schema
    def get_projects_schema(self):
        schema = StructType([
            StructField("id", 	                IntegerType(),False),	 	#_C0
            StructField("url", 	                StringType(),True),	 	    #_C1
            StructField("owner_id", 	        IntegerType(),True),	 	#_C2
            StructField("name", 	            StringType(),True),	 	    #_C3
            StructField("description", 	        StringType(),True),	 	    #_C4
            StructField("language", 	        StringType(),True),	 	    #_C5
            StructField("created_at", 	        TimestampType(),True),	 	#_C6
            StructField("forked_from", 	        IntegerType(),True),	 	#_C7
            StructField("deleted", 	            BooleanType(),True),	 	#_C8
            StructField("updated_at", 	        TimestampType(),True),      #_C9
            StructField("forked_commit_id", 	IntegerType(),True)	 	    #_C10
        ])
        return schema

    # Method to return the repo_lables schema
    def get_repo_label_schema(self):
        schema = StructType([
            StructField("id", 	        IntegerType(),False),	 	#_C0
            StructField("repo_id", 	    IntegerType(),True),	 	#_C1
            StructField("name", 	    StringType(),True)	 	    #_C2
        ])
        return schema

    # Method to return the users schema
    def get_users_schema(self):
        schema = StructType([
            StructField("id", 	     IntegerType(),False),	 	       #_C0
            StructField("login",     StringType(),True), 		   #_c1
            StructField("company",   StringType(),True), 		       #_c2
            StructField("created_at", TimestampType(),True),             #_c3
            StructField("type", 	 StringType(),True),        #_c4
            StructField("fake", 	 IntegerType(),True),            #_c5
            StructField("deleted", 	 IntegerType(),True),		   #_c6
            StructField("long", 	 DecimalType(),True),	   #_c7
            StructField("lat", 	     DecimalType(),True),		   #_c8
            StructField("country_code", StringType(),True),
            StructField("state",     StringType(),True),
            StructField("city",      StringType(),True),		       #_c9
            StructField("location",  StringType(),True)
        ])
        return schema
