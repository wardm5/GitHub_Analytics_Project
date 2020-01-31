from PostgresReader.Reader import *

sql = Reader()

sql.run_query("""SELECT * from default_2 where id = 5 """)
sql.print_results()
