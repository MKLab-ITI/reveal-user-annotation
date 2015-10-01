# -*- coding: <UTF-8> -*-
__author__ = 'Georgios Rizos (georgerizos@iti.gr)'

import time
import numpy as np
import scipy.sparse as sparse
import copy
import twython
from collections import defaultdict
from operator import itemgetter
from urllib.error import URLError
from http.client import BadStatusLine

from reveal_user_annotation.text.text_util import augmented_tf_idf, simple_word_query
from reveal_user_annotation.text.clean_text import clean_single_word
from reveal_user_annotation.twitter.twitter_util import login, safe_twitter_request_handler
from reveal_user_annotation.twitter.clean_twitter_list import user_twitter_list_bag_of_words
from reveal_user_annotation.twitter.manage_resources import get_reveal_set, get_topic_keyword_dictionary


def extract_user_keywords_generator(twitter_lists_gen, lemmatizing="wordnet"):
    """
    Based on the user-related lists I have downloaded, annotate the users.

    Inputs: - twitter_lists_gen: A python generator that yields a user Twitter id and a generator of Twitter lists.
            - lemmatizing: A string containing one of the following: "porter", "snowball" or "wordnet".

    Yields: - user_twitter_id: A Twitter user id.
            - user_annotation: A python dictionary that contains two dicts:
                * bag_of_lemmas: Maps emmas to multiplicity.
                * lemma_to_keywordbag: A python dictionary that maps stems/lemmas to original topic keywords.
    """
    ####################################################################################################################
    # Extract keywords serially.
    ####################################################################################################################
    for user_twitter_id, twitter_lists_list in twitter_lists_gen:
        if twitter_lists_list is not None:
            if "lists" in twitter_lists_list.keys():
                twitter_lists_list = twitter_lists_list["lists"]

            bag_of_lemmas, lemma_to_keywordbag = user_twitter_list_bag_of_words(twitter_lists_list, lemmatizing)

            for lemma, keywordbag in lemma_to_keywordbag.items():
                lemma_to_keywordbag[lemma] = dict(keywordbag)
            lemma_to_keywordbag = dict(lemma_to_keywordbag)

            user_annotation = dict()
            user_annotation["bag_of_lemmas"] = bag_of_lemmas
            user_annotation["lemma_to_keywordbag"] = lemma_to_keywordbag

            yield user_twitter_id, user_annotation


def form_user_label_matrix(user_twitter_list_keywords_gen, id_to_node, max_number_of_labels):
    """
    Forms the user-label matrix to be used in multi-label classification.

    Input:   - user_twitter_list_keywords_gen:
             - id_to_node:  A Twitter id to node map as a python dictionary.

    Outputs: - user_label_matrix: A user-to-label matrix in scipy sparse matrix format.
             - annotated_nodes: A numpy array containing graph nodes.
             - label_to_lemma: A python dictionary that maps a numerical label to a string topic lemma.
             - lemma_to_keyword: A python dictionary that maps a lemma to the original keyword.
    """
    user_label_matrix, annotated_nodes, label_to_lemma, node_to_lemma_tokeywordbag = form_user_term_matrix(user_twitter_list_keywords_gen,
                                                                                                           id_to_node,
                                                                                                           None)

    user_label_matrix, annotated_nodes, label_to_lemma = filter_user_term_matrix(user_label_matrix,
                                                                                 annotated_nodes,
                                                                                 label_to_lemma,
                                                                                 max_number_of_labels)

    lemma_to_keyword = form_lemma_tokeyword_map(annotated_nodes, node_to_lemma_tokeywordbag)

    return user_label_matrix, annotated_nodes, label_to_lemma, lemma_to_keyword


def write_terms_and_frequencies(path, user_term_matrix, label_to_lemma):
    from collections import OrderedDict

    bag_of_words = defaultdict(int)

    matrix = sparse.coo_matrix(user_term_matrix)

    for e in range(matrix.getnnz()):
        term = matrix.col[e]
        lemma = label_to_lemma[term]
        multiplicity = matrix.data[e]

        bag_of_words[lemma] += multiplicity

    bag_of_words = dict(bag_of_words)
    bag_of_words = OrderedDict(sorted(bag_of_words.items(), key=lambda t: t[1]))

    with open(path, "w") as fp:
        for key, value in bag_of_words.items():
            row = str(key) + "\t" + str(value) + "\n"
            fp.write(row)


def form_user_term_matrix(user_twitter_list_keywords_gen, id_to_node, lemma_set=None, keyword_to_topic_manual=None):
    """
    Forms a user-term matrix.

    Input:   - user_twitter_list_keywords_gen: A python generator that yields a user Twitter id and a bag-of-words.
             - id_to_node:  A Twitter id to node map as a python dictionary.
             - lemma_set: For the labelling, we use only lemmas in this set. Default: None

    Outputs: - user_term_matrix: A user-to-term matrix in scipy sparse matrix format.
             - annotated_nodes: A numpy array containing graph nodes.
             - label_to_topic: A python dictionary that maps a numerical label to a string topic/keyword.
             - node_to_lemma_tokeywordbag: A python dictionary that maps nodes to lemma-to-keyword bags.
    """
    # Prepare for iteration.
    term_to_attribute = dict()

    user_term_matrix_row = list()
    user_term_matrix_col = list()
    user_term_matrix_data = list()

    append_user_term_matrix_row = user_term_matrix_row.append
    append_user_term_matrix_col = user_term_matrix_col.append
    append_user_term_matrix_data = user_term_matrix_data.append

    annotated_nodes = list()
    append_node = annotated_nodes.append

    node_to_lemma_tokeywordbag = dict()

    invalid_terms = list()
    counter = 0

    if keyword_to_topic_manual is not None:
        manual_keyword_list = list(keyword_to_topic_manual.keys())

    for user_twitter_id, user_annotation in user_twitter_list_keywords_gen:
        counter += 1
        # print(counter)
        bag_of_words = user_annotation["bag_of_lemmas"]
        lemma_to_keywordbag = user_annotation["lemma_to_keywordbag"]

        if lemma_set is not None:
            bag_of_words = {lemma: multiplicity for lemma, multiplicity in bag_of_words.items() if lemma in lemma_set}
            lemma_to_keywordbag = {lemma: keywordbag for lemma, keywordbag in lemma_to_keywordbag.items() if lemma in lemma_set}

        node = id_to_node[user_twitter_id]
        append_node(node)
        node_to_lemma_tokeywordbag[node] = lemma_to_keywordbag
        for term, multiplicity in bag_of_words.items():

            if keyword_to_topic_manual is not None:
                keyword_bag = lemma_to_keywordbag[term]
                term = max(keyword_bag.keys(), key=(lambda key: keyword_bag[key]))

                found_list_of_words = simple_word_query(term, manual_keyword_list, edit_distance=1)

                if len(found_list_of_words) > 0:
                    term = found_list_of_words[0]

                try:
                    term = keyword_to_topic_manual[term]
                except KeyError:
                    print(term)

            vocabulary_size = len(term_to_attribute)
            attribute = term_to_attribute.setdefault(term, vocabulary_size)

            append_user_term_matrix_row(node)
            append_user_term_matrix_col(attribute)
            append_user_term_matrix_data(multiplicity)

    annotated_nodes = np.array(list(set(annotated_nodes)), dtype=np.int64)

    user_term_matrix_row = np.array(user_term_matrix_row, dtype=np.int64)
    user_term_matrix_col = np.array(user_term_matrix_col, dtype=np.int64)
    user_term_matrix_data = np.array(user_term_matrix_data, dtype=np.float64)

    user_term_matrix = sparse.coo_matrix((user_term_matrix_data,
                                          (user_term_matrix_row, user_term_matrix_col)),
                                         shape=(len(id_to_node), len(term_to_attribute)))

    label_to_topic = dict(zip(term_to_attribute.values(), term_to_attribute.keys()))

    # print(user_term_matrix.shape)
    # print(len(label_to_topic))
    # print(invalid_terms)

    return user_term_matrix, annotated_nodes, label_to_topic, node_to_lemma_tokeywordbag


def filter_user_term_matrix(user_term_matrix, annotated_nodes, label_to_topic, max_number_of_labels=None):
    """
    Filters out labels that are either too rare, or have very few representatives.

    Inputs:  - user_term_matrix: A user-to-term matrix in scipy sparse matrix format.
             - annotated_nodes: A numpy array containing graph nodes.
             - label_to_topic: A python dictionary that maps a numerical label to a string topic/keyword.

    Outputs: - user_term_matrix: The filtered user-to-term matrix in scipy sparse matrix format.
             - annotated_nodes: A numpy array containing graph nodes.
             - label_to_topic: A python dictionary that maps a numerical label to a string topic/keyword.
    """
    # Aggregate edges and eliminate zeros.
    user_term_matrix = user_term_matrix.tocsr()
    user_term_matrix.eliminate_zeros()

    # Calculate the label-user counts.
    temp_matrix = copy.copy(user_term_matrix)
    temp_matrix.data = np.ones_like(temp_matrix.data).astype(np.int64)
    label_distribution = temp_matrix.sum(axis=0)
    index = np.argsort(np.squeeze(np.asarray(label_distribution)))

    # Apply max number of labels cutoff.
    if max_number_of_labels is not None:
        if index.size > max_number_of_labels:
            index = index[:index.size-max_number_of_labels]
    else:
        index = np.array(list())

    # If there are zero samples of a label, remove them from both the matrix and the label-to-topic map.

    # percentile = 90
    # p = np.percentile(label_distribution, percentile)
    # index = np.where(label_distribution <= p)[1]
    if index.size > 0:
        index = np.squeeze(np.asarray(index))
        index = np.setdiff1d(np.arange(label_distribution.size), index)
        user_term_matrix = user_term_matrix[:, index]
        temp_map = dict()
        counter = 0
        for i in index:
            temp_map[counter] = label_to_topic[i]
            counter += 1
        label_to_topic = temp_map

    # Perform tf-idf on all matrices.
    user_term_matrix = augmented_tf_idf(user_term_matrix)

    # Most topics are heavy-tailed. For each topic, threshold to annotate.
    percentile = 80

    matrix_row = list()
    matrix_col = list()
    extend_matrix_row = matrix_row.extend
    extend_matrix_col = matrix_col.extend

    user_term_matrix = sparse.csc_matrix(user_term_matrix)
    for topic in range(user_term_matrix.shape[1]):
        col = user_term_matrix.getcol(topic)
        p = np.percentile(col.data, percentile)
        index = col.indices[col.data >= p]
        extend_matrix_row(index)
        extend_matrix_col(topic*np.ones_like(index))
    matrix_row = np.array(matrix_row, dtype=np.int64)
    matrix_col = np.array(matrix_col, dtype=np.int64)
    matrix_data = np.ones_like(matrix_row, dtype=np.int8)
    user_term_matrix = sparse.coo_matrix((matrix_data, (matrix_row, matrix_col)), shape=user_term_matrix.shape)
    user_term_matrix = sparse.csr_matrix(user_term_matrix)

    # If there is a small number of samples of a label, remove them from both the matrix and the label-to-topic map.
    temp_matrix = copy.copy(user_term_matrix)
    temp_matrix.data = np.ones_like(temp_matrix.data).astype(np.int64)
    label_distribution = temp_matrix.sum(axis=0)
    index = np.where(label_distribution < 31)[1]  # Fewer than 10.
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

    # print(user_term_matrix.shape)
    # print(len(label_to_topic))

    return user_term_matrix, annotated_nodes, label_to_topic


def semi_automatic_user_annotation(user_twitter_list_keywords_gen, id_to_node):

    reveal_set = get_reveal_set()
    topic_keyword_dict = get_topic_keyword_dictionary()

    available_topics = set(list(topic_keyword_dict.keys()))

    keyword_list = list()
    for topic in reveal_set:
        if topic in available_topics:
            keyword_list.extend(topic_keyword_dict[topic])

    lemma_set = list()
    for keyword in keyword_list:
        lemma = clean_single_word(keyword, lemmatizing="wordnet")
        lemma_set.append(lemma)
    lemma_set = set(lemma_set)

    keyword_topic_dict = dict()
    for topic, keyword_set in topic_keyword_dict.items():
        for keyword in keyword_set:
            keyword_topic_dict[keyword] = topic

    user_label_matrix, annotated_nodes, label_to_lemma, node_to_lemma_tokeywordbag = form_user_term_matrix(user_twitter_list_keywords_gen, id_to_node, lemma_set=lemma_set, keyword_to_topic_manual=keyword_topic_dict)


    user_label_matrix, annotated_nodes, label_to_lemma = filter_user_term_matrix(user_label_matrix,
                                                                                 annotated_nodes,
                                                                                 label_to_lemma,
                                                                                 max_number_of_labels=None)

    lemma_to_keyword = form_lemma_tokeyword_map(annotated_nodes, node_to_lemma_tokeywordbag)

    return user_label_matrix, annotated_nodes, label_to_lemma, lemma_to_keyword


def form_lemma_tokeyword_map(annotated_nodes, node_to_lemma_tokeywordbag):
    """
    Forms the aggregated dictionary that maps lemmas/stems to the most popular topic keyword.

    Inputs: - annotated_nodes: A numpy array of anonymized nodes that are annotated.
            - node_to_lemma_tokeywordbag: A map from nodes to maps from lemmas to bags of keywords.

    Output: - lemma_to_keyword: A dictionary that maps lemmas to keywords.
    """
    # Reduce relevant lemma-to-original keyword bags.
    lemma_to_keywordbag = defaultdict(lambda: defaultdict(int))
    for node in annotated_nodes:
        for lemma, keywordbag in node_to_lemma_tokeywordbag[node].items():
            for keyword, multiplicity in keywordbag.items():
                lemma_to_keywordbag[lemma][keyword] += multiplicity

    lemma_to_keyword = dict()
    for lemma, keywordbag in lemma_to_keywordbag.items():
        lemma_to_keyword[lemma] = max(keywordbag.items(), key=itemgetter(1))[0]

    return lemma_to_keyword


def fetch_twitter_lists_for_user_ids_generator(twitter_app_key,
                                               twitter_app_secret,
                                               user_id_list):
    """
    Collects at most 500 Twitter lists for each user from an input list of Twitter user ids.

    Inputs: - twitter_app_key: What is says on the tin.
            - twitter_app_secret: Ditto.
            - user_id_list: A python list of Twitter user ids.

    Yields: - user_twitter_id: A Twitter user id.
            - twitter_lists_list: A python list containing Twitter lists in dictionary (json) format.
    """
    ####################################################################################################################
    # Log into my application.
    ####################################################################################################################
    twitter = login(twitter_app_key,
                    twitter_app_secret)

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


def decide_which_users_to_annotate(centrality_vector,
                                   number_to_annotate,
                                   already_annotated,
                                   node_to_id):
    """
    Sorts a centrality vector and returns the Twitter user ids that are to be annotated.

    Inputs: - centrality_vector: A numpy array vector, that contains the centrality values for all users.
            - number_to_annotate: The number of users to annotate.
            - already_annotated: A python set of user twitter ids that have already been annotated.
            - node_to_id: A python dictionary that maps graph nodes to user twitter ids.

    Output: - user_id_list: A python list of Twitter user ids.
    """
    # Sort the centrality vector according to decreasing centrality.
    centrality_vector = np.asarray(centrality_vector)
    ind = np.argsort(np.squeeze(centrality_vector))
    if centrality_vector.size > 1:
        reversed_ind = ind[::-1]
    else:
        reversed_ind = list()
        reversed_ind = reversed_ind.append(ind)

    # Get the sublist of Twitter user ids to return.
    user_id_list = list()
    append_user_id = user_id_list.append

    counter = 0
    for node in reversed_ind:
        user_twitter_id = node_to_id[node]
        if user_twitter_id not in already_annotated:
            append_user_id(user_twitter_id)
            counter += 1

            if counter >= number_to_annotate:
                break

    return user_id_list


def on_demand_annotation(twitter_app_key, twitter_app_secret, user_twitter_id):
    """
    A service that leverages twitter lists for on-demand annotation of popular users.

    TODO: Do this.
    """
    ####################################################################################################################
    # Log into my application
    ####################################################################################################################
    twitter = login(twitter_app_key, twitter_app_secret)

    twitter_lists_list = twitter.get_list_memberships(user_id=user_twitter_id, count=1000)

    for twitter_list in twitter_lists_list:
        print(twitter_list)

    return twitter_lists_list
