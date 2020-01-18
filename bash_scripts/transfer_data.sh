#!/bin/bash
# A Simple Shell Script download from file and pipe output to S3 Bucket
# Misha Ward - 17/Jan/2020

wget -O- "https://data.ok.gov/sites/default/files/unspsc%20codes_3.csv" | aws s3 cp - s3://github-analysis-project/data-file/testing.csv
