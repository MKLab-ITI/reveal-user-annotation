__author__ = 'Georgios Rizos (georgerizos@iti.gr)'

from reveal_user_annotation.mongo.store_snow_data import store_snow_tweets_from_disk_to_mongodb
from reveal_user_annotation.mongo.mongo_util import start_local_mongo_daemon


def main(snow_tweets_folder):
    daemon = start_local_mongo_daemon()
    daemon.start()

    store_snow_tweets_from_disk_to_mongodb(snow_tweets_folder)

    daemon.join()