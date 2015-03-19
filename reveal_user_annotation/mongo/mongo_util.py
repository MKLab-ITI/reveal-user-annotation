__author__ = 'Georgios Rizos (georgerizos@iti.gr)'

import multiprocessing
import subprocess
import os
import pymongo

from reveal_user_annotation.common.config_package import get_package_path
from reveal_user_annotation.common.datarw import get_file_row_generator


def start_local_mongo_daemon():
    """
    Assuming that the executable mongod is in the PATH environment variable, we start the local mongo daemon.

    The file mongodb.config should have already been configured and the path must exist.

    Output: - daemon: This is a forked process object that runs the mongo daemon. The caller is responsible for joining.
    """
    file_row_gen = get_file_row_generator(get_package_path() + "/mongo/res/mongod_path.txt", "=")
    file_row = next(file_row_gen)
    executable_path = os.path.normpath(file_row[1])

    mongodb_config_path = os.path.normpath(get_package_path() + "/mongo/res/mongodb.config")

    daemon = multiprocessing.Process(target=subprocess.call,
                                     args=([executable_path, "--config", mongodb_config_path],))
    daemon.daemon = True
    return daemon


def daemon_wrap(daemon_already_running, function_handle, *args, **kw):
    """
    If the mongo daemon is not already running locally, we wrap an input function within a local call.

    Inputs: - daemon_already_running: Actually start the Mongo daemon only if it is not already running.
            - function_handle: A python Mongo function.
            - *args, **kw: The function's arguments.

    Output: - function_result: The result tuple of the function.
    """
    if daemon_already_running:
        function_result = function_handle(*args, **kw)
    else:
        daemon = start_local_mongo_daemon()
        daemon.start()

        function_result = function_handle(*args, **kw)

        daemon.join()

    return function_result


def establish_mongo_connection(mongo_uri):
    """
    What it says on the tin.

    Inputs: - mongo_uri: A MongoDB URI.

    Output: - A MongoDB client.
    """

    client = pymongo.MongoClient(mongo_uri)
    return client


def delete_database(host_name, port_name, database_name):
    """
    Delete a mongo database using pymongo. Mongo daemon assumed to be running.

    Inputs: - host_name: The host name where the mongo database is located.
            - port_name: The port through which the mongo databse is exposed.
            - database_name: The mongo database name as a python string.
    """
    client = pymongo.MongoClient(host_name, port_name)

    client.drop_database(database_name)


def delete_collection(host_name, port_name, database_name, collection_name):
    """
    Delete a mongo document collection using pymongo. Mongo daemon assumed to be running.

    Inputs: - host_name: The host name where the mongo database is located.
            - port_name: The port through which the mongo databse is exposed.
            - database_name: The mongo database name as a python string.
            - collection_name: The mongo collection as a python string.
    """
    client = pymongo.MongoClient(host_name, port_name)

    db = client[database_name]

    db.drop_collection(collection_name)
