from PostgresReader.Reader import *
sql = Reader()

def run_query():
    return sql.run_query("""SELECT * from default_2 where id = 5 """)

sql.run_query("""SELECT * from default_2 where id = 5 """)
sql.print_results()
