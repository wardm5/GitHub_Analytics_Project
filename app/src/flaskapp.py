# https://www.datasciencebytes.com/bytes/2015/02/24/running-a-flask-app-on-aws-ec2/

# from flask import Flask
# app = Flask(__name__)
#
# @app.route('/')
# def hello_world():
#   return 'Hello from Flask!'
#
# if __name__ == '__main__':
#   app.run()
from PostgresReader.Reader import *
sql = Reader()

from flask import Flask
app = Flask(__name__)

@app.route('/')
def hello_world():

    return sql.run_query("""SELECT * from persons """)
	# return 'Hello, World!'

@app.route('/countme/<input_str>')
def count_me(input_str):
    return input_str

if __name__ == "__main__":
    app.run()
