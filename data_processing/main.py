from DataProcessor.Processor import *

# setup processor program
program = Processor('test')
# read tables from S3
program.read_from_default_tables()
# preprocess tables (remove unnecessary columns)
program.preprocess_tables()
# show the current table names that you can work with
program.get_table_names()
users = program.get_table('users')
users.show()

program.create_pie_chart_data()

# project.create_project_tables()
# program.create_bar_chart_of_langauge_table()
# program.write_specific_table_to_postgres('pie_chart_table')

def create_project_tables():
    print("nothing here yet")
    # percentile for commits

def write_default_tables_to_postgres():
    program.write_specific_table_to_postgres('default_1')
    program.write_specific_table_to_postgres('default_2')
    program.write_specific_table_to_postgres('default_3')
