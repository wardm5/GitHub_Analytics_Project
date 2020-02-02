import psycopg2
import pandas as pd
import pandas.io.sql as sqlio

class Reader():
    # Constructor for the Reader, this reads from PostgreSQL database or provides an error message.
    def __init__(self):
        try:
            self.conn = psycopg2.connect("dbname='test' user='postgres' host='database-1.cu6pvppk2zw2.us-west-2.rds.amazonaws.com' password='Trotsky1'")
            self.query = self.conn.cursor()
            print("Status: Connected to database")
        except:
            print("Status: Failed to connect to database")
            self.conn = None
            self.query = None
        self.results = None

    # Method allows one to try to reconnect to the database if a failure occurred.
    def reconnect_to_database(self):
        try:
            self.conn = psycopg2.connect("dbname='test' user='postgres' host='database-1.cu6pvppk2zw2.us-west-2.rds.amazonaws.com' password='Trotsky1'")
        except:
            self.conn = None
            print("Status: connection failed. ")

    # Method allows one to run a query to the SQL database and returns the results from that query
    def run_query(self, query):
        self.results = sqlio.read_sql_query(query, self.conn)
        # self.query.execute(query)
        # self.results = self.query.fetchall()
        return self.results

    # Method prints out the results from the last SQL query or provides error message noting no results stored
    def print_results(self):
        if (self.results is not None):
            print("Status: no results stored")
            return
        else:
            print(self.results)
