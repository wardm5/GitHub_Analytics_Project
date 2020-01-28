import psycopg2
conn = None
conn = psycopg2.connect("dbname='test' user='postgres' host='database-1.cu6pvppk2zw2.us-west-2.rds.amazonaws.com' password='Trotsky1'")
cur = conn.cursor()
cur.execute("""SELECT * from persons """)
rows = cur.fetchall()

print "\nShow me the databases:\n"
for row in rows:
    print "   ", row[0]
    print "   ", row[1]
    print "   ", row[2]
