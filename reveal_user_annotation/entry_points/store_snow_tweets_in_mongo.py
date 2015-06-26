__author__ = 'Georgios Rizos (georgerizos@iti.gr)'

import argparse

from reveal_user_annotation.mongo.store_snow_data import store_snow_tweets_from_disk_to_mongodb


def main():
    # Parse arguments.
    parser = argparse.ArgumentParser()
    parser.add_argument("-f", "--folder", dest="snow_tweets_folder",
                        help="This is the folder with the SNOW tweets.",
                        type=str, required=True)

    args = parser.parse_args()

    snow_tweets_folder = args.snow_tweets_folder

    store_snow_tweets_from_disk_to_mongodb(snow_tweets_folder)
