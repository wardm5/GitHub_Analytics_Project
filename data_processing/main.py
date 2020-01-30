from DataProcessor.Processor import *

# setup processor program
program = Processor()
# read tables from S3
program.read_from_tables()
# preprocess tables (remove unnecessary columns)
program.preprocess_tables()

program.show_table('test')

program.count_table_rows()

users = program.get_table('commits')
users.show()
