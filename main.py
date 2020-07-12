""" main - This is the main module for the MongoDB data recovery tool, calling the client, getIDs and getDocs modules. """

# Script: main.py
# Desc: The main module for the MongoDB data recovery tool. First establishes a connection
#       with the target MongoDB instance using the client module. Search criteria is provided
#       by the user and passed to the getIDs module, which returns workable ObjectIds. These
#       ObjectIds are then passed to the getDocs module which extracts documents with a matching
#       ObjectIds, these are then written to a log file.
# Author: Liam Short
# Created: 10/10/2017
# Stage: Finished

# Imported modules
import getIDs
import getDocs
import client
import os
import sys
import time
import datetime
from datetime import datetime
import pymongo
import socket
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure

# Prevent traceback error ouput
sys.tracebacklimit = None

# clear terminal
os.system('clear')

# welcome message
print("MongoDB Data Recovery Tool \nMongoDB 3.4.9 \nUbuntu 16.04 \nDeveloped by Liam Short")

def main():
    """ This is the main function of the module, from here all the other modules and functions are called.
        First the client module is called and the tool will attempt to establish a connection with the
        target MongoDB instance. When connected the user is given the option to attempt data recovery or
        terminate the connection(if further functionality is added to the tool, other options can be added
        to this area). If the user attempts data recovery, the user input modules are called. The user input
        details is then passed to the getIDs module, which returns ObjectIds. These ObjectIds are passed to
        the getDocs module which writes its findings to the log file.
        If the user chooses to terminate the connection, the client module is called again.
        """

    # get connection details from user
    print("-----------------------------------------------------------")
    server_address, server_port = input("\nEnter MongoDB Client Address and Port: ").split(':')
    
    connection = client.connect(server_address, server_port)
    
    serverVersion = client.check_version(connection)

    print("\n-----------------------------------------------------------")        

    while (1):

        # connection established
        print ("\nCONNECTED TO: "+server_address +":"+ server_port)

        # connect to the oplog.rs collection within the local database
        db = connection.local.oplog.rs

        # option selection
        selection = input("\n1 - Attempt data recovery \n2 - Close connection \n\nSelect: ")

        # option 1, attempt data recovery
        if selection == '1':

            while (1):

                print("\n-----------------------------------------------------------")

                output, oplog_operation, oplog_database, oplog_collection = search_criteria()

                if oplog_operation == "i":
                    op = ("INSERTED")
                if oplog_operation == "u":
                    op = ("UPDATED")
                if oplog_operation == "d":
                    op = ("DELETED")
                
                datetime_start = datetime_range("Start")
                datetime_end = datetime_range("End")

                file_name, log_file = create_log()

                log_file.write("Conncted to Server: " +
                    server_address + ":" + server_port +
                    "\nMongoDB Version: " + str(serverVersion) +
                    "\n\nSearching for " + op +
                    " documents in, \nDatabase: " + oplog_database +
                    "\nCollection: " + oplog_collection +
                    "\n\nBetween Times,\nStart: " + str(datetime_start) +
                    "\nEnd: " + str(datetime_end) +
                    "\n\n-----------------------\n")
        
                print ("\nRetreiving IDs...")

                object_id, cursor_count = getIDs.id(log_file, db, datetime_start, datetime_end, oplog_operation, oplog_database, oplog_collection)

                print (("\nIdentified "+ str(cursor_count) + " " + op + " document(s)"))

                if object_id:
                    contains = True
                else:
                    print("\nNo matching entries recovered")
                    print("\n-----------------------------------------------------------")               
                    break

                recovery_message = ("\nAttempting to recover " + op + " entries...")

                start = time.time()
   
                # initiate get_inserted to retreive inserted entries
                if oplog_operation == 'i':
                    print(recovery_message)
                    getDocs.insert(log_file, db, object_id)

                # initiate get_updated to retreive updated entries
                if oplog_operation == 'u':
                    print(recovery_message)
                    getDocs.update(log_file, db, object_id)

                # initiate get_deleted to retreive deleted entries
                if oplog_operation == 'd' and contains is True:
                    print(recovery_message)
                    getDocs.delete(log_file, db, object_id)
                    
                log_file.close()

                end = time.time()

                print("\n-----------------------\n")

                if output in ('y', 'yes'):
                    read_log(file_name)

                print ("Results in Log: " + file_name)

                print ("\nTotal Recovery Time: " + str(end - start))
                    
                # Give user the option to return to main menu?
                print("\n-----------------------------------------------------------")               
                flag = input('\nContinue with data recovery? ')
                if (flag[0].upper() == 'N'):
                    print("\n-----------------------------------------------------------")               
                    break

        # terminate the connection
        if selection == '2':
            client.disconnect(connection)

def create_log():
    """ This function creates a log directory named "logs" if one does not exist already.
        It then creates a log file, which contains the date and time it was created.
        The name of the file is returned to allow it be read, and the open log is returned
        which allows it to be written to.
        """

    if not os.path.exists('logs'):
        os.makedirs('logs')

    datestring = datetime.strftime(datetime.now(), '(%d-%m-%Y_%H:%M:%S)')
    file_name = ("log"+datestring+".txt")
    log_file = open(os.path.join('logs', file_name), 'w')
    
    return file_name, log_file

def read_log(file_name):
    """ This function reads the contents of the log file if specified by the user.
        Within the logs directory, the log file name is used to specify which file to
        print to the screen.
        """

    read_log = open(os.path.join('logs', file_name), 'r')
    data = read_log.read()
    print(data)
    read_log.close()

def search_criteria():
    """ This function takes user input; if the user would like to print the
        findings to the screen, are they recovering insert, update or delete entries
        and finally the names of the database and collection the data belonged to.
        """
    
    while True:
        
        try:
            output = input("\nWould you like to print findings to screen? ").lower()
        except ValueError:
            print("\Invalid Input\n")
            continue
        if output in ('y', 'n', 'yes', 'no'):
            print("\n-----------------------")
            break
                                    
    while True:
        print("\nINSERTED data enter \"i\"" +
              "\nUPDATED data enter \"u\"" +
              "\nDELETED data enter \"d\"")
        try:
            oplog_operation = input("\nRecover what type of data: ").lower()
        except ValueError:
            print("\nInvalid operation stated\n")
            continue
        if oplog_operation in ('i', 'u', 'd'):
            print("\n-----------------------")
            break
                    
    oplog_database = input("\nRecover data from which database: ")
    if not oplog_database:
        print("\nNo database stated")
    print("\n-----------------------")

    oplog_collection = input("\nRecover data from which collection: ")
    if not oplog_collection:
        print("\nNo collection stated")
    print("\n-----------------------")

    return output, oplog_operation, oplog_database, oplog_collection

def datetime_range(mode=""):
    """ This function takes user input to determine the start and end datetime used
        when extracting the ObjectIds.
        """

    while True:
        
        print("\nDatetime in DD-MM-YYYY HH:MM:SS format")
        
        try:
            oplog_datetime = datetime.strptime(input('{}: '.format(mode)), '%d-%m-%Y %H:%M:%S')
        except (ValueError, TypeError):
            print ("\nInvalid datetime format")
        else:
            print("\n-----------------------")
            break

    return oplog_datetime

# Standard Boilerplate To Call The main() Function To Begin The Program
# This Code Runs First
if __name__ == '__main__':
    main()
