#!/bin/bash
# A Simple Shell Script download from file and pipe output to S3 Bucket
# Misha Ward - 17/Jan/2020

wget -O- "http://ghtorrent-downloads.ewi.tudelft.nl/mysql/mysql-2016-01-16.tar.gz" | tar -xz  | aws s3 cp - s3://github-analysis-project/data-file/testing


#  aws s3 cp s3://github-analysis-project/data-file/testing.tar.gz - | tar -xz  | aws s3 cp s3://github-analysis-project/data-file/

#  tar -xz aws s3 cp s3://github-analysis-project/data-file/testing.tar.gz | aws s3 cp - s3://github-analysis-project/data-file/
# tar -zxvf aws s3 cp s3://github-analysis-project/github.tar.gz
# tar -zxvf aws s3 cp s3://github-analysis-project/github.tar.gz

# aws s3 cp s3://github-analysis-project/github.tar.gz - | tar -xz | aws s3 cp - s3://github-analysis-project/example-data
# tar -zxvf

wget -c "http://ghtorrent-downloads.ewi.tudelft.nl/mysql/mysql-2016-01-16.tar.gz" | tar -zxvf- | aws s3 cp - s3://github-analysis-project/data-file/testing123
wget -c "http://ghtorrent-downloads.ewi.tudelft.nl/mysql/mysql-2019-06-01.tar.gz" | tar -xz


# wget -qO- "http://ghtorrent-downloads.ewi.tudelft.nl/mysql/mysql-2015-06-18.tar.gz" | tar -zxvf- | aws s3 cp - s3://github-analysis-project/data-file/testing123
 tar -xzf "http://ghtorrent-downloads.ewi.tudelft.nl/mysql/mysql-2016-01-16.tar.gz"


# wget -O- "https://data.ok.gov/sites/default/files/unspsc%20codes_3.csv" | aws s3 cp - s3://github-analysis-project/data-file/testing.csv
