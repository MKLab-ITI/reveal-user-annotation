__author__ = 'Georgios Rizos (georgerizos@iti.gr)'

import pymongo


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
