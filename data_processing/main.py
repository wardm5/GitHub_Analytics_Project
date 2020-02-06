from DataProcessor.Processor import *
import config

# setup processor program --- Parameters: S3 Bucket Path, EC2 DNS, User, DB Password
program = Processor('github-analysis-project/final-data', 'database-1.cu6pvppk2zw2.us-west-2.rds.amazonaws.com', '5432', 'postgres', config.password)
# read tables from S3
program.read_from_default_tables()
# preprocess tables (remove unnecessary columns)
program.preprocess_tables()
# show the current table names that you can work with

# program.calculate_top_languages()     # 'languages_data'
# program.create_pie_chart_data()       # 'pie_chart_data' - Should be used for Lanaguage breakdown, project table, and byte size comparison
# program.calculate_top_cities()          # 'cities_data'
program.calculate_commits()           # 'commits_users_data'

program.get_table_names()
# program.write_specific_table_to_postgres('pie_chart_data')
# program.write_specific_table_to_postgres('pie_chart_data')
# program.write_specific_table_to_postgres('cities_data')
program.write_specific_table_to_postgres('commits_users_data')
