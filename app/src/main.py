from PostgresReader.Reader import *
sql = Reader()

def run_query():
    return sql.run_query("SELECT login as Login, language as Language, Count from default_2 where id = 148")

sql.run_query("""SELECT * from default_2 where id = 5 """)
sql.print_results()
