__author__ = 'Georgios Rizos (georgerizos@iti.gr)'

import pymongo


def establish_mongo_connection(mongo_uri):
    """
    What it says on the tin. Mongo daemon assumed to be running.

    Inputs: - mongo_uri: A MongoDB URI.

    Output: - A MongoDB client.
    """
    client = pymongo.MongoClient(mongo_uri)
    return client


def delete_database(mongo_uri, database_name):
    """
    Delete a mongo database using pymongo. Mongo daemon assumed to be running.

    Inputs: - mongo_uri: A MongoDB URI.
            - database_name: The mongo database name as a python string.
    """
    client = pymongo.MongoClient(mongo_uri)

    client.drop_database(database_name)


def delete_collection(mongo_uri, database_name, collection_name):
    """
    Delete a mongo document collection using pymongo. Mongo daemon assumed to be running.

    Inputs: - mongo_uri: A MongoDB URI.
            - database_name: The mongo database name as a python string.
            - collection_name: The mongo collection as a python string.
    """
    client = pymongo.MongoClient(mongo_uri)

    db = client[database_name]

    db.drop_collection(collection_name)
