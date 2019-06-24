#!/usr/bin/env python3

'''
This script converts MongoDB records for the Workspace Shock backend into records for the
workspace S3 backend. The script does not alter the Shock backend records and may be re-run
multiple times without issue.

To run:
1) Start the workspace at least once with the S3 backend enabled to create the appropriate
    MongoDB indexes.
2) fill in the configuration variables for mongo DB below and run the script normally.
'''

###### CONFIGURATION VARIABLES ######

CONFIG_MONGO_HOST = "localhost"
CONFIG_MONGO_DATABASE = "workspace"
#CONFIG_MONGO_DATABASE = "workspace_conv_test"
#CONFIG_MONGO_DATABASE = "workspace_conv_test_many_recs"
CONFIG_MONGO_USER = ""
CONFIG_MONGO_PWD = ""

#### END CONFIGURATION VARIABLES ####

from pymongo.mongo_client import MongoClient

COLLECTION_SHOCK = "shock_nodeMap"
COLLECTION_S3 = "s3_objects"

KEY_SHOCK_CHKSUM = "chksum"
KEY_SHOCK_NODE = "node"
KEY_SHOCK_SORTED = "sorted"

KEY_S3_CHKSUM = "chksum"
KEY_S3_KEY = "key"
KEY_S3_SORTED = "sorted"

'''
Potential improvement: allow filtering by > object id. Then you can run this script while
the workspace is up (since it could take hours), figure out the last object id processed, bring
the ws down, and then run with the object id filter to just process the new records.
'''
def main():
    if CONFIG_MONGO_USER:
        client = MongoClient(CONFIG_MONGO_HOST, authSource=CONFIG_MONGO_DATABASE,
            username=CONFIG_MONGO_USER, password=CONFIG_MONGO_PWD)
    else:
        client = MongoClient(CONFIG_MONGO_HOST)

    db = client[CONFIG_MONGO_DATABASE]
    ttl = db[COLLECTION_SHOCK].count_documents({})
    count = 0
    lastPrint = "Processed {}/{} records".format(count, ttl)
    print(lastPrint, end='', flush=True)
    for o in db[COLLECTION_SHOCK].find():
        db[COLLECTION_S3].update_one(
            {KEY_S3_CHKSUM: o[KEY_SHOCK_CHKSUM]},
            {"$set": {
                KEY_S3_KEY: toS3Key(o[KEY_SHOCK_NODE]),
                KEY_S3_SORTED: True if o.get(KEY_SHOCK_SORTED) else False}},
            upsert=True)
        count += 1
        if count % 100 == 0:
            backspace = '\b' * len(lastPrint)
            lastPrint = "Processed {}/{} records".format(count, ttl)
            print(backspace + lastPrint, end='', flush=True)

    backspace = '\b' * len(lastPrint)
    lastPrint = "Processed {}/{} records".format(count, ttl)
    print(backspace + lastPrint)

def toS3Key(node):
    return node[0:2] + '/' + node[2:4] + '/' + node[4:6] + '/' + node

if __name__ == "__main__":
    main()
