""" client.py - This module contains functions for connecting to a MongoDB instance using criteria provided by the main functon, checking the MongoDB version and termnating the connection. """

# Script: client.py
# Desc: This module connects to the MongoDB instance using the details provided by the main module and returns
#       the open connection. It also checks the version of the MongoDB instance, terminating the connection if
#       it is below version 2.6.0, and terminating the connection if requested by the user.
# Author: Liam Short
# Created: 10/10/2017
# Stage: Finished

# imported module starts here
import pymongo
import socket
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure

def connect(server_address, server_port):
    """ This function attempts to connect to the target MongoDB instance using the
        IP and Port provided in the main script.
        If the connection is timesout or is not possible the user is notified.
        """
        
    try:
        # attempt connection to mongodb client and return connection details to main
        connection = MongoClient(server_address+':'+server_port)
        socket.inet_aton(server_address)

    except ConnectionFailure:
        print("\n-----------------------------------------------------------")
        print("MONGODB CLIENT NOT AVAILABLE")
        print("-----------------------------------------------------------")
        quit()

    return connection

def check_version(connection):
    """ This functon checks the version of the connected MongoDB instance, if it is
        found to be below version 2.6.0 the connection is terminated and the user is notified.
        """

    # check the MongoDB version and return it to main
    serverVersion = tuple(connection.server_info()['version'].split('.'))
    requiredVersion = tuple("2.6.0".split("."))
    if serverVersion < requiredVersion:
        print("\n-----------------------------------------------------------")
        print("MONGODB VERSION NOT COMPATIBLE")
        print("-----------------------------------------------------------")
        quit()
        
    return serverVersion


def disconnect(connection):
    """ This function terminates the connection and notifies the user. """

    # close the connection to MongoDB
    connection.close()
    print("\n-----------------------------------------------------------")
    print("CONNECTION TERMINATED")
    print("-----------------------------------------------------------")
    quit()
