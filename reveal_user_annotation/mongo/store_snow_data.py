__author__ = 'Georgios Rizos (georgerizos@iti.gr)'

import pymongo
import json
import os


def extract_snow_tweets_from_file_generator(json_file_path):
    """
    A generator that opens a file containing many json tweets and yields all the tweets contained inside.

    Input:  - json_file_path: The path of a json file containing a tweet in each line.

    Yields: - tweet: A tweet in python dictionary (json) format.
    """
    with open(json_file_path, "r", encoding="utf-8") as fp:
        for file_line in fp:
            tweet = json.loads(file_line)
            yield tweet


def extract_all_snow_tweets_from_disk_generator(json_folder_path):
    """
    A generator that returns all SNOW tweets stored in disk.

    Input:  - json_file_path: The path of the folder containing the raw data.

    Yields: - tweet: A tweet in python dictionary (json) format.
    """
    # Get a generator with all file paths in the folder
    json_file_path_generator = (json_folder_path + "/" + name for name in os.listdir(json_folder_path))

    for path in json_file_path_generator:
        for tweet in extract_snow_tweets_from_file_generator(path):
            yield tweet


def store_snow_tweets_from_disk_to_mongodb(snow_tweets_folder):
    """
    Store all SNOW tweets in a mongodb collection.
    """
    client = pymongo.MongoClient("localhost", 27017)

    db = client["snow_tweet_storage"]
    collection = db["tweets"]

    for tweet in extract_all_snow_tweets_from_disk_generator(snow_tweets_folder):
        collection.insert(tweet)
