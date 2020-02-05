# spark-submit --driver-memory 8g --executor-memory 8g --master spark://ec2-44-227-43-63.us-west-2.compute.amazonaws.com:7077 main.py

# option 1, run as just one core
python main.py
# option 2, run as spark cluster
spark-submit --master spark://ec2-44-227-43-63.us-west-2.compute.amazonaws.com:7077 --jars /home/ubuntu/postgresql-42.2.9.jar main.py hdfs://ec2-44-227-43-63.us-west-2.compute.amazonaws.com:9000
