from DataProcessor.Processor import *

# setup processor program
program = Processor('test')
# read tables from S3
program.read_from_default_tables()
# preprocess tables (remove unnecessary columns)
program.preprocess_tables()
# show the current table names that you can work with

program.create_pie_chart_data()
program.get_table_names()
program.write_specific_table_to_postgres('pie_chart_data')
