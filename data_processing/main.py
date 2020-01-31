from DataProcessor.Processor import *

# setup processor program
program = Processor()
# read tables from S3
program.read_from_tables()
# preprocess tables (remove unnecessary columns)
program.preprocess_tables()

program.create_percentile_table()


# program.show_table('users')
# program.count_table_rows()

# commits = program.get_table('commits')
# projects = program.get_table('projects')
# users = program.get_table('users')
# users.show()

# commits.printSchema()
# commits.show()

# print("Status: joining users and commits tables")
# commit = commits.alias('commits')
# user = users.alias('users')
# inner_join = commit.join(user, commit.committer_id == user.id).select(user["login"],commit["*"])
# print("inner join table count: " , inner_join.count())
# inner_join.show()
