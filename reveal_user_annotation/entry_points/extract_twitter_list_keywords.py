__author__ = 'Georgios Rizos (georgerizos@iti.gr)'

import argparse
import os
from multiprocessing import Pool
from functools import partial
import json

from reveal_user_annotation.common.config_package import get_threads_number
from reveal_user_annotation.common.datarw import load_pickle
from reveal_user_annotation.text.map_data import chunks
from reveal_user_annotation.twitter.clean_twitter_list import user_twitter_list_bag_of_words
from reveal_user_annotation.text.clean_text import get_lemmatizer, get_stopset, get_camel_case_regexes,\
    get_digits_punctuation_whitespace_regex, get_pos_set, get_braupt_tagger, get_tokenizer


def worker_function(file_name_list,
                    source_folder,
                    target_folder):
    source_path_list = (source_folder + "/" + file_name for file_name in file_name_list)
    target_path_list = (target_folder + "/" + file_name[:-4] + ".json" for file_name in file_name_list)

    sent_tokenize, _treebank_word_tokenize = get_tokenizer()
    tagger = get_braupt_tagger()
    lemmatizer, lemmatize = get_lemmatizer("wordnet")
    stopset = get_stopset()
    first_cap_re, all_cap_re = get_camel_case_regexes()
    digits_punctuation_whitespace_re = get_digits_punctuation_whitespace_regex()
    pos_set = get_pos_set()

    # Get the lists of a user
    for source_path in source_path_list:
        twitter_lists_corpus = load_pickle(source_path)
        if "lists" in twitter_lists_corpus.keys():
            twitter_lists_corpus = twitter_lists_corpus["lists"]
        else:
            continue

        bag_of_lemmas, lemma_to_keywordbag = user_twitter_list_bag_of_words(twitter_lists_corpus,
                                                                            sent_tokenize, _treebank_word_tokenize,
                                                                            tagger, lemmatizer, lemmatize, stopset,
                                                                            first_cap_re, all_cap_re, digits_punctuation_whitespace_re,
                                                                            pos_set)

        user_annotation = dict()
        user_annotation["bag_of_lemmas"] = bag_of_lemmas
        user_annotation["lemma_to_keywordbag"] = lemma_to_keywordbag

        target_path = next(target_path_list)
        with open(target_path, "w", encoding="utf-8") as fp:
            json.dump(user_annotation, fp)


def main():
    # Parse arguments.
    parser = argparse.ArgumentParser()
    parser.add_argument("-s", "--source", dest="source_folder",
                        help="This is the folder with the pickled Twitter lists.",
                        type=str, required=True)
    parser.add_argument("-t", "--target", dest="target_folder",
                        help="This is the folder where the extracted keyword jsons will be stored.",
                        type=str, required=True)

    args = parser.parse_args()

    source_folder = args.source_folder
    target_folder = args.target_folder

    # Get the file names where the twitter lists for certain users are stored.
    file_name_list = os.listdir(source_folder)

    # Build a pool of processes.
    pool = Pool(processes=get_threads_number()*2,)

    # Partition dataset in chunks.
    user_chunks = chunks(file_name_list, get_threads_number()*2)

    # Extract bags of words in parallel and serialize and store in JSON format.
    pool.map(partial(worker_function,
                     source_folder=source_folder,
                     target_folder=target_folder),
             user_chunks)
