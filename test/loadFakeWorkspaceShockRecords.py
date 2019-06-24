#!/usr/bin/env python3

'''
This script loads fake worksapce shock records into a DB to allow for manually testing the
print statements in workspaceShockToS3.py.
'''

###### CONFIGURATION VARIABLES ######

NUM_RECORDS_TO_LOAD = 1010
CONFIG_MONGO_HOST = "localhost"
CONFIG_MONGO_DATABASE = "workspace_conv_test_many_recs"

#### END CONFIGURATION VARIABLES ####

import random
import uuid
from pymongo.mongo_client import MongoClient

COLLECTION_SHOCK = "shock_nodeMap"

KEY_SHOCK_CHKSUM = "chksum"
KEY_SHOCK_NODE = "node"
KEY_SHOCK_SORTED = "sorted"

def main():
    client = MongoClient(CONFIG_MONGO_HOST)

    db = client[CONFIG_MONGO_DATABASE]
    
    for _ in range(NUM_RECORDS_TO_LOAD):
        chksum = '%032x' % random.getrandbits(128)
        db[COLLECTION_SHOCK].insert(
            {KEY_SHOCK_CHKSUM: chksum,
             KEY_SHOCK_NODE: str(uuid.uuid4()),
             KEY_SHOCK_SORTED: True})
        
if __name__ == "__main__":
    main()