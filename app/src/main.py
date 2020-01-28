from PostgresReader.Reader import *

sql = Reader()

sql.run_query("""SELECT * from persons """)
