# python imports
import boto3
from io import BytesIO
import gzip

# setup constants
bucket = 'github-analysis-project'
gzipped_key = 'http://ghtorrent-downloads.ewi.tudelft.nl/mysql/mysql-2016-02-01.tar.gz'
uncompressed_key = 'testFiles'

import contextlib;
import requests;
from io import BytesIO;

s3 = boto3.resource('s3');
s3Client = boto3.client('s3')
for bucket in s3.buckets.all():
  print(bucket.name)

# url = "https://data.ok.gov/sites/default/files/unspsc%20codes_3.csv"
url = "http://ghtorrent-downloads.ewi.tudelft.nl/mongo-full/users-dump.2015-12-01.tar.gz"
with contextlib.closing(requests.get(url, stream=True, verify=False)) as response:
        # Set up file stream from response content.
        fp = BytesIO(response.content)
        # print(response.content)
        # Upload data to S3
        # s3Client.upload_fileobj(fp, 'github-analysis-project', 'github.csv')
        s3Client.upload_fileobj(fp, 'github-analysis-project', 'github.tar.gz')

# # initialize s3 client, this is dependent upon your aws config being done
# # s3 = boto3.client('s3', use_ssl=False)  # optional
# s3.upload_fileobj(                      # upload a new obj to s3
#     Fileobj=gzip.GzipFile(              # read in the output of gzip -d
#         None,                           # just return output as BytesIO
#         'rb',                           # read binary
#         fileobj=BytesIO(s3.get_object(Bucket=bucket, Key=gzipped_key)['Body'].read())),
#     Bucket=bucket,                      # target bucket, writing to
#     Key=uncompressed_key)               # target key, writing to
#
# # read the body of the s3 key object into a string to ensure download
# s = s3.get_object(Bucket=bucket, Key=gzip_key)['Body'].read()
# print(len(s))  # check to ensure some data was returned
