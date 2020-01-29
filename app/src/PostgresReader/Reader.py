import psycopg2
class Reader():
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

    def reconnect_to_database(self):
        try:
            self.conn = psycopg2.connect("dbname='test' user='postgres' host='database-1.cu6pvppk2zw2.us-west-2.rds.amazonaws.com' password='Trotsky1'")
        except:
            self.conn = None
            print("Status: connection failed. ")

    def run_query(self, query):
        self.query.execute(query)
        self.results = self.query.fetchall()
        return self.results

    def print_results(self):
        if (self.results == None):
            print("Status: no results stored")
            return
        else:
            for row in self.results:
                print "   ", row[0]
                print "   ", row[1]
                print "   ", row[2]
