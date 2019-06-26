#!/usr/bin/env python3

'''
This script converts a subset of Shock (https://github.com/kbase/Shock) node records to
BlobStore (https://github.com/kbase/blobstore) records.
The script does not alter the Shock backend records and may be re-run multiple times without issue.

The script determines which files to transfer by looking in the S3 bucket that contains the
converted Shock files. It expects key names based on a uuid in the same way Shock directories are
based on the UUID; for example the UUID 06f5d3ec-8ebf-4d32-8c1c-41e27e40b7fd would be

06/f5/d3/06f5d3ec-8ebf-4d32-8c1c-41e27e40b7fd

... as a key in S3 or a directory tree in Shock.

To use:
1) Start the blobstore at least once so that the proper indexes are created in MongoDB.
2) Fill in the configuration variables below and run the script normally.
'''

###### CONFIGURATION VARIABLES ######

CONFIG_MONGO_SHOCK_HOST = 'localhost'
CONFIG_MONGO_SHOCK_USER = ''
CONFIG_MONGO_SHOCK_PWD = ''
CONFIG_MONGO_SHOCK_DATABASE = 'ShockDB'

CONFIG_MONGO_BLOBSTORE_HOST = 'localhost'
CONFIG_MONGO_BLOBSTORE_USER = ''
CONFIG_MONGO_BLOBSTORE_PWD = ''
CONFIG_MONGO_BLOBSTORE_DATABASE = 'bs_test'

CONFIG_S3_HOST = 'http://localhost:9000'
# The bucket name must obey https://docs.aws.amazon.com/AmazonS3/latest/dev/BucketRestrictions.html
# with the extra restriction that periods are not allowed.
CONFIG_S3_BUCKET = 'blobstore'
CONFIG_S3_ACCESS_KEY = 'access key goes here'
CONFIG_S3_ACCESS_SECRET = 'access secret goes here'
CONFIG_S3_REGION = 'us-west-1'

#### END CONFIGURATION VARIABLES ####

import boto3
import uuid
import botocore.config as bcfg
from pymongo.mongo_client import MongoClient

BS_COL_NODES = 'nodes'
BS_COL_USERS = 'users'

BS_KEY_USERS_ID = 'id'
BS_KEY_USERS_USER = 'user'

BS_KEY_NODES_ID = 'id'
BS_KEY_NODES_OWNER = 'own'
BS_KEY_NODES_READERS = 'read'
BS_KEY_NODES_FILENAME = 'fname'
BS_KEY_NODES_FORMAT = 'fmt'
BS_KEY_NODES_SIZE = 'size'
BS_KEY_NODES_MD5 = 'md5'
BS_KEY_NODES_STORED = 'time'
BS_KEY_NODES_PUBLIC = 'pub'


SHOCK_COL_NODES = 'Nodes'
SHOCK_COL_USERS = 'Users'

SHOCK_KEY_USERS_ID = 'uuid'
SHOCK_KEY_USERS_USER = 'username'

SHOCK_KEY_NODES_ID = 'id'
SHOCK_KEY_NODES_CREATED = 'created_on'
SHOCK_KEY_NODES_FILE = 'file'
SHOCK_KEY_NODES_FILE_NAME = 'name'
SHOCK_KEY_NODES_FILE_SIZE = 'size'
SHOCK_KEY_NODES_FILE_CHKSUM = 'checksum'
SHOCK_KEY_NODES_FILE_CHKSUM_MD5 = 'md5'
SHOCK_KEY_NODES_FILE_FORMAT = 'format'
SHOCK_KEY_NODES_ACLS = 'acl'
SHOCK_KEY_NODES_ACLS_OWNER = 'owner'
SHOCK_KEY_NODES_ACLS_READERS = 'read'
SHOCK_KEY_NODES_ACLS_PUBLIC = 'public'

def main():
    shockdb = get_client(CONFIG_MONGO_SHOCK_HOST, CONFIG_MONGO_SHOCK_DATABASE,
        CONFIG_MONGO_SHOCK_USER, CONFIG_MONGO_SHOCK_PWD)[CONFIG_MONGO_SHOCK_DATABASE]
    bsdb = get_client(CONFIG_MONGO_BLOBSTORE_HOST, CONFIG_MONGO_BLOBSTORE_DATABASE,
        CONFIG_MONGO_BLOBSTORE_USER, CONFIG_MONGO_BLOBSTORE_PWD)[CONFIG_MONGO_BLOBSTORE_DATABASE]

    s3 = boto3.client(
        's3',
        endpoint_url=CONFIG_S3_HOST,
        aws_access_key_id=CONFIG_S3_ACCESS_KEY,
        aws_secret_access_key=CONFIG_S3_ACCESS_SECRET,
        region_name=CONFIG_S3_REGION,
        config=bcfg.Config(s3={'addressing_style': 'path'})
    )

    paginator = s3.get_paginator('list_objects_v2')
    seenusers = {}

    # no way to get object count in a bucket other than listing them, apparently

    count = 0
    lastPrint = ''
    for page in paginator.paginate(Bucket=CONFIG_S3_BUCKET):
        nodes = [toUUID(o['Key']) for o in page['Contents']]
        for n in nodes:
            node = shockdb[SHOCK_COL_NODES].find_one({'id': n})
            if not node:
                raise ValueError("Missing shock node " + n)
            bsnode = toBSNode(node, seenusers, shockdb, bsdb)
            bsdb[BS_COL_NODES].update_one({BS_KEY_NODES_ID: n}, {'$set': bsnode}, upsert=True)
            count += 1
            if count % 100 == 0:
                backspace = '\b' * len(lastPrint)
                lastPrint = 'Processed {} records'.format(count)
                print(backspace + lastPrint, end='', flush=True)

    backspace = '\b' * len(lastPrint)
    lastPrint = 'Processed {} records'.format(count)
    print(backspace + lastPrint)

def toBSNode(shocknode, seenusers, shockdb, bsdb):
    n = shocknode
    owner = n[SHOCK_KEY_NODES_ACLS][SHOCK_KEY_NODES_ACLS_OWNER]
    md5 = n[SHOCK_KEY_NODES_FILE][SHOCK_KEY_NODES_FILE_CHKSUM][SHOCK_KEY_NODES_FILE_CHKSUM_MD5]
    readers = n[SHOCK_KEY_NODES_ACLS][SHOCK_KEY_NODES_ACLS_READERS]
    pub = SHOCK_KEY_NODES_ACLS_PUBLIC in readers
    while SHOCK_KEY_NODES_ACLS_PUBLIC in readers: readers.remove(SHOCK_KEY_NODES_ACLS_PUBLIC)  

    bsnode = {
        BS_KEY_NODES_ID: n[SHOCK_KEY_NODES_ID],
        BS_KEY_NODES_OWNER: get_user(owner, seenusers, shockdb, bsdb),
        BS_KEY_NODES_READERS: [get_user(r, seenusers, shockdb, bsdb) for r in readers],
        BS_KEY_NODES_STORED: n[SHOCK_KEY_NODES_CREATED],
        BS_KEY_NODES_FILENAME: n[SHOCK_KEY_NODES_FILE][SHOCK_KEY_NODES_FILE_NAME],
        BS_KEY_NODES_SIZE: n[SHOCK_KEY_NODES_FILE][SHOCK_KEY_NODES_FILE_SIZE],
        BS_KEY_NODES_FORMAT: n[SHOCK_KEY_NODES_FILE][SHOCK_KEY_NODES_FILE_FORMAT],
        BS_KEY_NODES_MD5: md5,
        BS_KEY_NODES_PUBLIC: pub,
    }
    return bsnode

def get_user(uuid, seenusers, shockdb, bsdb):
    if uuid in seenusers:
        return {BS_KEY_USERS_ID: uuid, BS_KEY_USERS_USER: seenusers[uuid]}
    
    u = shockdb[SHOCK_COL_USERS].find_one({SHOCK_KEY_USERS_ID: uuid})
    if not u:
        raise ValueError('Missing user in Shock ' + uuid)
    bsdb[BS_COL_USERS].update_one(
        {BS_KEY_USERS_ID: uuid},
        {'$set': {BS_KEY_USERS_USER: u[SHOCK_KEY_USERS_USER]}},
        upsert=True)
    seenusers[uuid] = u[SHOCK_KEY_USERS_USER]
    return {BS_KEY_USERS_ID: uuid, BS_KEY_USERS_USER: u[SHOCK_KEY_USERS_USER]}
    
def toUUID(s3key):
    u = s3key.split('/')
    uuidStr = u[3]
    if uuidStr[0:2] != u[0] or uuidStr[2:4] != u[1] or uuidStr[4:6] != u[2]:
        raise ValueError("Illegal S3 key: " + uuidStr)
    try:
        uuid.UUID(hex=uuidStr)
    except TypeError as _:
        raise ValueError("Illegal S3 key: " + uuidStr)
    return uuidStr
    
def get_client(host, db, user, pwd):
    if user:
        return MongoClient(host, authSource=db, username=user, password=pwd)
    else:
        return MongoClient(host)

if __name__ == '__main__':
    main()

