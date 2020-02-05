from DataProcessor.Processor import *

# setup processor program
program = Processor('test')
# read tables from S3
program.read_from_default_tables()
# preprocess tables (remove unnecessary columns)
program.preprocess_tables()
# show the current table names that you can work with

# program.calculate_top_languages()
# program.create_pie_chart_data()       # Should be used for Lanaguage breakdown, project table, and byte size comparison
program.calculate_top_cities()
program.get_table_names()
program.write_specific_table_to_postgres('cities_data')
# program.write_specific_table_to_postgres('pie_chart_data')
