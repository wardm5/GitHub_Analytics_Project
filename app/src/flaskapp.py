# https://www.datasciencebytes.com/bytes/2015/02/24/running-a-flask-app-on-aws-ec2/

from flask import Flask
app = Flask(__name__)

@app.route('/')
def hello_world():
  return 'Hello from Flask!'

if __name__ == '__main__':
  app.run()
