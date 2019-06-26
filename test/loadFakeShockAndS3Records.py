#!/usr/bin/env python3

'''
This script loads fake shock records and S3 records into their respective DBs to allow for manually
testing the pagination in shockMongoRecordsToS3.py.
'''

RECORD_COUNT = 1010

SHOCK_NODE_URL = "http://localhost:7044/node"

CONFIG_S3_HOST = 'http://localhost:9000'
# The bucket name must obey https://docs.aws.amazon.com/AmazonS3/latest/dev/BucketRestrictions.html
# with the extra restriction that periods are not allowed.
CONFIG_S3_BUCKET = 'blobstore'
CONFIG_S3_ACCESS_KEY = 'access key goes here'
CONFIG_S3_ACCESS_SECRET = 'access secret goes here'
CONFIG_S3_REGION = 'us-west-1'

import io
import sys
import boto3
import botocore.config as bcfg
import requests


def main():
    token = sys.argv[1]

    s3 = boto3.client(
        's3',
        endpoint_url=CONFIG_S3_HOST,
        aws_access_key_id=CONFIG_S3_ACCESS_KEY,
        aws_secret_access_key=CONFIG_S3_ACCESS_SECRET,
        region_name=CONFIG_S3_REGION,
        config=bcfg.Config(s3={'addressing_style': 'path'})
    )

    for _ in range(RECORD_COUNT):
        ret = requests.post(SHOCK_NODE_URL, data=io.StringIO("foo"),
            headers={'authorization': 'oauth ' + token})
        j = ret.json()
        nid = j['data']['id']
        key = nid[0:2] + '/' + nid[2:4] + '/' + nid[4:6] + '/' + nid
        _ = s3.put_object(
            Body=io.BytesIO(b'whee'),
            Bucket=CONFIG_S3_BUCKET,
            Key=key)
        
if __name__ == '__main__':
    main()