""" getDocs - Extracts entries from the Oplog using the ObjectIds returned by the getIDs module """

# Script: getDocs.py
# Desc: This module extracts entries from the Oplog using the ObjectIds returned by the getIDs module.
#       Any matching entries are processed and written to the log file.
# Author: Liam Short
# Created: 10/10/2017
# Stage: Finished

# imported module starts here
import os
import re
import sys
from pprint import pprint
import time
from datetime import datetime
import pymongo
import json
from bson import ObjectId

# prevent traceback error ouput
sys.tracebacklimit = None

def delete(log_file, db, object_id):
    """ This function searches the Oplog for insert, update and delete entries with the ObjectId
    returned by the getIDs module. Found entries are passed to the relevent function to be
    processed, extracting information, then written to the log file.
    """

    while(1):

        for index, delete_id in enumerate(object_id, 1):

            cursor_insert = db.find({'o._id' : ObjectId(delete_id), 'op': 'i'},{'o':1, 'ts':1})
            cursor_update = db.find({'o2._id' : ObjectId(delete_id), 'op': 'u'},{'o':1, 'o2':1, 'ts':1})
            cursor_delete = db.find({'o._id' : ObjectId(delete_id), 'op': 'd'},{'o':1, 'ts':1})

            for delete_id in cursor_insert:
                log_insert(index, delete_id, log_file)

            for delete_id in cursor_update:
                log_update(index, delete_id, log_file)

            for delete_id in cursor_delete:
                log_delete(index, delete_id, log_file)

            log_file.write("\n-----------------------\n")
            
        break

        # clear object_id after data recovery to prevent future false positives
        object_id.clear()
        break

def update(log_file, db, object_id):
    """ This function searches the Oplog for insert and update entries with the ObjectId
    returned by the getIDs module. Found entries are passed to the relevent function to be
    processed, extracting information, then written to the log file.
    """

    while(1):
        
        for index, update_id in enumerate(object_id, 1):

            cursor_insert = db.find({'o._id' : ObjectId(update_id), 'op': 'i'},{'o':1, 'ts':1})
            cursor_update = db.find({'o2._id' : ObjectId(update_id), 'op': 'u'},{'o':1, 'o2':1, 'ts':1})

            for update_id in cursor_insert:
                log_insert(index, update_id, log_file)

            for update_id in cursor_update:
                log_update(index, update_id, log_file)

            log_file.write("\n-----------------------\n")
            
        break
            
        # clear object_id after data recovery to prevent future false positives
        object_id.clear()
        break

def insert(log_file, db, object_id):
    """ This function searches the Oplog for insert, update and delete entries wiht the ObjectId
    returned by the getIDs module. Found entries are passed to the log_insert function to be
    processed, extracting information, then written to the log file.
    """
    
    while(1):
        
        for index, insert_id in enumerate(object_id, 1):
            
            cursor_insert = db.find({'o._id' : ObjectId(insert_id), 'op': 'i'},{'o':1, 'ts':1})

            for insert_id in cursor_insert:
                log_insert(index, insert_id, log_file)

            log_file.write("\n-----------------------\n")
            
        break

        # clear object_id after data recovery to prevent future false positives
        object_id.clear()
        break

def log_insert(index, insert_id, log_file):
    """ This function processes the entries that were found in the insert, update and delete
    functions and writes the output to the log file. Regex is used to to extract the timestamp
    and convert it to human readable format.
    """

    string_ts = re.findall(r'\Timestamp\(([0-9]{10})', str(insert_id))
    clean_ts = re.sub('[^a-f0-9]+', '', str(string_ts))
    insert_time = time.strftime('%d-%m-%Y %H:%M:%S', time.localtime(int(clean_ts)))
                                    
    log_file.write("\nDocument " + str(index) + " - INSERTED at " + insert_time + "\n")
    pprint(insert_id, log_file)

def log_update(index, update_id, log_file):
    """ This function processes the entries that were found in the insert and update functions
    and writes the output to the log file. Regex is used to to extract the timestamp and convert
    it to human readable format.
    """

    string_ts = re.findall(r'\Timestamp\(([0-9]{10})', str(update_id))
    clean_ts = re.sub('[^a-f0-9]+', '', str(string_ts))
    update_time = time.strftime('%d-%m-%Y %H:%M:%S', time.localtime(int(clean_ts)))

    log_file.write("\nDocument " + str(index) + " - UPDATED at " + update_time + "\n")
    pprint(update_id, log_file)

def log_delete(index, delete_id, log_file):
    """ This function processes the entries that were found in the delete functions and
    writes the output to the log file. Regex is used to to extract the timestamp and convert
    it to human readable format.
    """
    
    string_ts = re.findall(r'\Timestamp\(([0-9]{10})', str(delete_id))
    clean_ts = re.sub('[^a-f0-9]+', '', str(string_ts))
    delete_time = time.strftime('%d-%m-%Y %H:%M:%S', time.localtime(int(clean_ts)))

    log_file.write("\nDocument " + str(index) + " - DELETED at " + delete_time + "\n")
    pprint(delete_id, log_file)

# Standard Boilerplate To Call The main() Function To Begin The Program
# This Code Runs First
if __name__ == '__main__':
    main()
