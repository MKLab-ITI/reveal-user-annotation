# -*- coding: <UTF-8> -*-
__author__ = 'Georgios Rizos (georgerizos@iti.gr)'

import time
import numpy as np
import scipy.sparse as sparse
import copy
import twython
from urllib.error import URLError
from http.client import BadStatusLine

from reveal_user_annotation.text.text_util import augmented_tf_idf
from reveal_user_annotation.twitter.twitter_util import login, safe_twitter_request_handler
from reveal_user_annotation.twitter.clean_twitter_list import user_twitter_list_bag_of_words


def extract_user_keywords_generator(twitter_lists_gen, lemmatizing="wordnet"):
    """
    Based on the user-related lists I have downloaded, annotate the users.

    Inputs: - twitter_lists_gen: A python generator that yields a user Twitter id and a generator of Twitter lists.
            - lemmatizing: A string containing one of the following: "porter", "snowball" or "wordnet".

    Yields: - user_twitter_id: A Twitter user id.
            - bag_of_words: A python dictionary that maps keywords/lemmas to multiplicity.
    """
    ####################################################################################################################
    # Extract keywords serially.
    ####################################################################################################################
    for user_twitter_id, twitter_lists_list in twitter_lists_gen:
        bag_of_lemmas, lemma_to_keywordbag = user_twitter_list_bag_of_words(twitter_lists_list, lemmatizing)

        user_annotation = dict()
        user_annotation["bag_of_lemmas"] = bag_of_lemmas
        user_annotation["lemma_to_keywordbag"] = lemma_to_keywordbag

        yield user_twitter_id, user_annotation


def form_user_label_matrix(user_twitter_list_keywords_gen):
    """
    Forms the user-label matrix to be used in multi-label classification.

    Input:   - user_twitter_list_keywords_gen:

    Outputs: - user_label_matrix: A user-to-label matrix in scipy sparse matrix format.
             - annotated_user_ids: A numpy array containing the user Twitter ids.
             - label_to_topic: A python dictionary that maps a numerical label to a string topic/keyword.
    """
    user_label_matrix, annotated_user_ids, label_to_topic = form_user_term_matrix(user_twitter_list_keywords_gen)

    user_label_matrix, annotated_user_ids, label_to_topic = filter_user_term_matrix(user_label_matrix,
                                                                                    annotated_user_ids,
                                                                                    label_to_topic)

    return user_label_matrix, annotated_user_ids, label_to_topic


def form_user_term_matrix(user_twitter_list_keywords_gen):
    """
    Forms a user-term matrix.

    Input:   - user_twitter_list_keywords_gen: A python generator that yields a user Twitter id and a bag-of-words.

    Outputs: - user_term_matrix: A user-to-term matrix in scipy sparse matrix format.
             - annotated_user_ids: A numpy array containing the user Twitter ids.
             - label_to_topic: A python dictionary that maps a numerical label to a string topic/keyword.
    """
    # Prepare for iteration.
    term_to_attribute = dict()

    user_term_matrix_row = list()
    user_term_matrix_col = list()
    user_term_matrix_data = list()

    append_user_term_matrix_row = user_term_matrix_row.append
    append_user_term_matrix_col = user_term_matrix_col.append
    append_user_term_matrix_data = user_term_matrix_data.append

    annotated_user_ids = list()
    append_user_id = annotated_user_ids.append

    for user_twitter_id, bag_of_words in user_twitter_list_keywords_gen:
        append_user_id(user_twitter_id)
        for term, multiplicity in bag_of_words:
            vocabulary_size = len(term_to_attribute)
            attribute = term_to_attribute.setdefault(term, default=vocabulary_size)

            append_user_term_matrix_row(user_twitter_id)
            append_user_term_matrix_col(attribute)
            append_user_term_matrix_data(multiplicity)

    annotated_user_ids = np.array(list(set(annotated_user_ids)), dtype=np.int64)

    user_term_matrix_row = np.array(user_term_matrix_row, dtype=np.int64)
    user_term_matrix_col = np.array(user_term_matrix_col, dtype=np.int64)
    user_term_matrix_data = np.array(user_term_matrix_data, dtype=np.float64)

    user_term_matrix = sparse.coo_matrix((user_term_matrix_data,
                                          (user_term_matrix_row, user_term_matrix_col)),
                                         shape=(annotated_user_ids.size, len(term_to_attribute)))

    label_to_topic = dict(zip(term_to_attribute.values(), term_to_attribute.keys()))

    return user_term_matrix, annotated_user_ids, label_to_topic


def filter_user_term_matrix(user_term_matrix, annotated_user_ids, label_to_topic):
    """
    Filters out labels that are either too rare, or have very few representatives.

    Inputs:  - user_term_matrix: A user-to-term matrix in scipy sparse matrix format.
             - annotated_user_ids: A numpy array containing the user Twitter ids.
             - label_to_topic: A python dictionary that maps a numerical label to a string topic/keyword.

    Outputs: - user_term_matrix: The filtered user-to-term matrix in scipy sparse matrix format.
             - annotated_user_ids: A numpy array containing the user Twitter ids.
             - label_to_topic: A python dictionary that maps a numerical label to a string topic/keyword.
    """
    # Eliminate zeros.
    user_term_matrix = user_term_matrix.tocsr()
    user_term_matrix.eliminate_zeros()

    # If there are zero samples of a label, remove them from both the matrix and the label-to-topic map.
    temp_matrix = copy.copy(user_term_matrix)
    temp_matrix.data = np.ones_like(temp_matrix.data).astype(np.int64)
    label_distribution = temp_matrix.sum(axis=0)
    index = np.where(label_distribution <= 0)[1]
    if index.shape[1] > 0:
        index = np.squeeze(np.asarray(index))
        index = np.setdiff1d(np.arange(label_distribution.size), index)
        user_term_matrix = user_term_matrix[:, index]
        temp_map = dict()
        counter = 0
        for i in index:
            temp_map[counter] = label_to_topic[i]
            counter += 1
        label_to_topic = temp_map

    # Perform tf-idf on all matrices
    user_term_matrix = augmented_tf_idf(user_term_matrix)

    # Most topics are heavy-tailed. For each topic, threshold to annotate.
    percentile = 75

    # Story topic tf-idf
    print("Story topic tf-idf.")
    matrix_row = list()
    matrix_col = list()
    for topic in np.arange(user_term_matrix.shape[1]):
        col = user_term_matrix.getcol(topic)
        p = np.percentile(col.data, percentile)
        index = col.indices[col.data >= p]
        matrix_row.extend(index)
        matrix_col.extend(topic*np.ones_like(index))
        print(col.data[col.data < p].size, col.data[col.data >= p].size)
    matrix_row = np.array(matrix_row, dtype=np.int64)
    matrix_col = np.array(matrix_col, dtype=np.int64)
    matrix_data = np.ones_like(matrix_row, dtype=np.int8)
    user_term_matrix = sparse.coo_matrix((matrix_data, (matrix_row, matrix_col)), shape=user_term_matrix.shape)

    # If there is a small number of samples of a label, remove them from both the matrix and the label-to-topic map.
    temp_matrix = copy.copy(user_term_matrix)
    temp_matrix.data = np.ones_like(temp_matrix.data).astype(np.int64)
    label_distribution = temp_matrix.sum(axis=0)
    index = np.where(label_distribution < 5)[1]
    if index.shape[1] > 0:
        index = np.squeeze(np.asarray(index))
        index = np.setdiff1d(np.arange(label_distribution.size), index)
        user_term_matrix = user_term_matrix[:, index]
        temp_map = dict()
        counter = 0
        for i in index:
            temp_map[counter] = label_to_topic[i]
            counter += 1
        label_to_topic = temp_map

    return user_term_matrix, annotated_user_ids, label_to_topic


def fetch_twitter_lists_for_user_ids_generator(user_id_list):
    """
    Collects at most 500 Twitter lists for each user from an input list of Twitter user ids.

    Input:  - user_id_list: A python list of Twitter user ids.

    Yields: - user_twitter_id: A Twitter user id.
            - twitter_lists_list: A python list containing Twitter lists in dictionary (json) format.
    """
    ####################################################################################################################
    # Log into my application.
    ####################################################################################################################
    twitter = login()

    ####################################################################################################################
    # For each user, gather at most 500 Twitter lists.
    ####################################################################################################################
    get_list_memberships_counter = 0
    get_list_memberships_time_window_start = time.perf_counter()
    for user_twitter_id in user_id_list:
        # Make safe twitter request.
        try:
            twitter_lists_list, get_list_memberships_counter, get_list_memberships_time_window_start\
                = safe_twitter_request_handler(twitter_api_func=twitter.get_list_memberships,
                                               call_rate_limit=15,
                                               call_counter=get_list_memberships_counter,
                                               time_window_start=get_list_memberships_time_window_start,
                                               max_retries=5,
                                               wait_period=2,
                                               user_id=user_twitter_id,
                                               count=500,
                                               cursor=-1)
            # If the call is succesful, yield the list of Twitter lists.
            yield user_twitter_id, twitter_lists_list
        except twython.TwythonError:
            # If the call is unsuccesful, we do not have any Twitter lists to store.
            yield user_twitter_id, None
        except URLError:
            # If the call is unsuccesful, we do not have any Twitter lists to store.
            yield user_twitter_id, None
        except BadStatusLine:
            # If the call is unsuccesful, we do not have any Twitter lists to store.
            yield user_twitter_id, None


def decide_which_users_to_annotate(centrality_vector, start_index=0, offset=100):
    """
    Sorts a centrality vector and returns the Twitter user ids that are to be annotated.

    Inputs: - centrality_vector: A numpy array vector, that contains the centrality values for all users.
            - start_index: An index that denotes the rank of the most central user to be annotated.
            - offset: The number of users in decreasing centrality to be annotated.

    Output: - user_id_list: A python list of Twitter user ids.
    """
    # Sort the centrality vector according to decreasing centrality.
    ind = np.argsort(centrality_vector)
    reversed_ind = ind[::-1]

    # Get the sublist of Twitter user ids to return.
    end_index = start_index + offset
    user_id_list = list(reversed_ind[start_index:end_index])

    return user_id_list


def on_demand_annotation(user_twitter_id):
    """
    A service that leverages twitter lists for on-demand annotation of popular users.

    TODO: Do this.
    """
    ####################################################################################################################
    # Log into my application
    ####################################################################################################################
    twitter = login()

    twitter_lists_list = twitter.get_list_memberships(user_id=user_twitter_id, count=1000)

    for twitter_list in twitter_lists_list:
        print(twitter_list)

    return twitter_lists_list
