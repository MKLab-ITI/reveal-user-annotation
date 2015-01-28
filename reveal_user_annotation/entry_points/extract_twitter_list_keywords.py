__author__ = 'Georgios Rizos (georgerizos@iti.gr)'

import os
from multiprocessing import Pool
from functools import partial
import json

from reveal_user_annotation.common.config_package import get_raw_datasets_path, get_memory_path, get_threads_number
from reveal_user_annotation.common.datarw import load_pickle
from reveal_user_annotation.text.map_data import chunks
from reveal_user_annotation.twitter.clean_twitter_list import user_twitter_list_bag_of_words


def worker_function(file_name_list,
                    lemmatizing="wordnet"):

    source_path_list = (get_raw_datasets_path() + "/SNOW/twitter_lists/" + file_name for file_name in file_name_list)
    target_path_list = (get_memory_path() + "/SNOW/twitter_lists/" + file_name[:-4] + ".json" for file_name in file_name_list)

    # Get the lists of a user
    for source_path in source_path_list:
        twitter_lists_corpus = load_pickle(source_path)
        if "lists" in twitter_lists_corpus.keys():
            twitter_lists_corpus = twitter_lists_corpus["lists"]
        else:
            continue

        bag_of_words = user_twitter_list_bag_of_words(twitter_lists_corpus, lemmatizing)

        target_path = next(target_path_list)
        with open(target_path, "w") as fp:
            json.dump(bag_of_words, fp)


def main():
    # Get the file names where the twitter lists for certain users are stored.
    file_name_list = os.listdir(get_raw_datasets_path() + "/twitter_lists")

    # Build a pool of processes.
    pool = Pool(processes=get_threads_number()*2,)

    # Partition dataset in chunks.
    user_chunks = chunks(file_name_list, get_threads_number()*2)

    # Extract bags of words in parallel and serialize and store in JSON format.
    pool.map(partial(worker_function,
                     lemmatizing="wordnet"),
             user_chunks)
