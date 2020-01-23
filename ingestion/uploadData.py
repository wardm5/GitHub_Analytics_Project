# python imports
import boto3
from io import BytesIO
import gzip


import urllib2

# url = 'http://winterolympicsmedals.com/medals.csv'


# setup constants
bucket = 'github-analysis-project'
gzipped_key = 'http://ghtorrent-downloads.ewi.tudelft.nl/mongo-full/users-dump.2015-12-01.tar.gz'
response = urllib2.urlopen(gzipped_key)
uncompressed_key = 'data-file'

# initialize s3 client, this is dependent upon your aws config being done
s3 = boto3.client('s3', use_ssl=False)  # optional
s3.upload_fileobj(                      # upload a new obj to s3
    Fileobj=gzip.GzipFile(              # read in the output of gzip -d
        None,                           # just return output as BytesIO
        'rb',                           # read binary
        fileobj=BytesIO(response),
    Bucket=bucket,                      # target bucket, writing to
    Key=uncompressed_key))               # target key, writing to

# read the body of the s3 key object into a string to ensure download
s = s3.get_object(Bucket=bucket, Key=gzip_key)['Body'].read()
print(len(s))  # check to ensure some data was returned
