""" getIDs - Searches the Oplog for entries that meet the criteria defined by the main module. The ObjectIds of any matches are returned to the main module. """

# Script: getIDs.py
# Desc: Searches the Oplog for entries that meet the criteria provided by the main module.
#       The ObjectIds of any matches are returned to the main module.
# Author: Liam Short
# Created: 10/10/2017
# Stage: Finished

# imported module starts here
import re
import sys
import time
import datetime
from datetime import datetime
import pymongo
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure

# prevent traceback error ouput
sys.tracebacklimit = None

def id(log_file, db, oplog_start_datetime, oplog_end_datetime, oplog_operation, oplog_database, oplog_collection):
    """ This function uses the search criteria provided by the user to create query operations to return
        the ObjectIds and timestamps of any matching entries. These are then passed to the extract_info function which
        returns the ObjectId in a workable format, which is returned to the main function.
        """

    # define set to store object id
    object_id = set()
        
    # seperate fielf for update entries 'o2'
    if oplog_operation == 'u':
        cursor = db.find({'op':{'$in':[oplog_operation]},'ns':{'$in':[oplog_database+"."+oplog_collection]}},{'o2._id':1,'ts':1})
    else:
        # find the object id of record
        cursor = db.find({'op':{'$in':[oplog_operation]},'ns':{'$in':[oplog_database+"."+oplog_collection]}},{'o._id':1,'ts':1})

    # cursor count
    cursor_count = cursor.count()

    # iterate over cursor
    for doc in enumerate(cursor, 1):

        clean_id = extract_info(doc, oplog_start_datetime, oplog_end_datetime)
        
        if clean_id not in object_id:
            object_id.add(clean_id)

    return object_id, cursor_count

def extract_info(doc, oplog_start_datetime, oplog_end_datetime):
    """ This function takes the ObjectIds and timestamps found by the id function. These are then covnerted
        to a workable format using regex. If the timestamp falls within the time range defined by the user
        the ObjectId is returned to the id function.
        """

    # regex for ts value then convert to datetime for range
    string_ts = re.findall(r'\Timestamp\(([0-9]{10})', str(doc))
    clean_ts = re.sub('[^a-f0-9]+', '', str(string_ts))
    
    string_dt = time.strftime('%d-%m-%Y %H:%M:%S', time.localtime(int(clean_ts)))
    datetime_object = datetime.strptime((string_dt), '%d-%m-%Y %H:%M:%S')

    if oplog_start_datetime <= datetime_object <= oplog_end_datetime:
        string_id = re.findall(r'\'([a-z0-9]{24})\'', str(doc))                    
        clean_id = re.sub('[^a-f0-9]+', '', str(string_id))

    return clean_id

# Standard Boilerplate To Call The main() Function To Begin The Program
# This Code Runs First
if __name__ == '__main__':
    main()
