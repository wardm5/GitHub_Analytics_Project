from DataProcessor.Processor import *

# setup processor program
program = Processor()
# read tables from S3
program.read_from_tables()
# preprocess tables (remove unnecessary columns)
program.preprocess_tables()
# show the current table names that you can work with
program.get_table_names()



def create_project_tables():
    # percentile for commits
    # program.create_default_table_1()
    # get commits per project
    program.create_default_table_2()
    # Create percentiles for each language
    # program.create_default_table_3()

def write_default_tables_to_postgres():
    program.write_specific_table_to_postgres('default_1')
    program.write_specific_table_to_postgres('default_2')
    program.write_specific_table_to_postgres('default_3')
