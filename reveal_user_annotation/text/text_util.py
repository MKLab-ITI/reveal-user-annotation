__author__ = 'Georgios Rizos (georgerizos@iti.gr)'

import numpy as np
import nltk


def combine_word_list(word_list):
    """
    Combine word list into a bag-of-words.

    Input:  - word_list: This is a python list of strings.

    Output: - bag_of_words: This is the corresponding multi-set or bag-of-words, in the form of a python dictionary.
    """
    bag_of_words = dict()

    for word in word_list:
        if word in bag_of_words.keys():
            bag_of_words[word] += 1
        else:
            bag_of_words[word] = 1

    return bag_of_words


def reduce_list_of_bags_of_words(list_of_keyword_sets):
    """
    Reduces a number of keyword sets to a bag-of-words.

    Input:  - list_of_keyword_sets: This is a python list of sets of strings.

    Output: - bag_of_words: This is the corresponding multi-set or bag-of-words, in the form of a python dictionary.
    """
    bag_of_words = dict()
    get_bag_of_words_keys = bag_of_words.keys
    for keyword_set in list_of_keyword_sets:
        for keyword in keyword_set:
            if keyword in get_bag_of_words_keys():
                bag_of_words[keyword] += 1
            else:
                bag_of_words[keyword] = 1

    return bag_of_words


def augmented_tf_idf(attribute_matrix):
    """
    Performs augmented TF-IDF normalization on a bag-of-words vector representation of data.

    Augmented TF-IDF introduced in: Manning, C. D., Raghavan, P., & Schütze, H. (2008).
                                    Introduction to information retrieval (Vol. 1, p. 6).
                                    Cambridge: Cambridge university press.

    Input:  - attribute_matrix: A bag-of-words vector representation in SciPy sparse matrix format.

    Output: - attribute_matrix: The same matrix after augmented tf-idf normalization.
    """
    number_of_documents = attribute_matrix.shape[0]

    max_term_frequencies = np.ones(number_of_documents, dtype=np.float64)
    idf_array = np.ones(attribute_matrix.shape[1], dtype=np.float64)

    # Calculate inverse document frequency
    attribute_matrix = attribute_matrix.tocsc()
    for j in range(attribute_matrix.shape[1]):
        document_frequency = attribute_matrix.getcol(j).data.size
        if document_frequency > 1:
            idf_array[j] = np.log(number_of_documents/document_frequency)

    # Calculate maximum term frequencies for a user
    attribute_matrix = attribute_matrix.tocsr()
    for i in range(attribute_matrix.shape[0]):
        max_term_frequency = attribute_matrix.getrow(i).data
        if max_term_frequency.size > 0:
            max_term_frequency = max_term_frequency.max()
            if max_term_frequency > 0.0:
                max_term_frequencies[i] = max_term_frequency

    # Do augmented tf-idf normalization
    attribute_matrix = attribute_matrix.tocoo()
    attribute_matrix.data = 0.5 + np.divide(0.5*attribute_matrix.data, np.multiply((max_term_frequencies[attribute_matrix.row]), (idf_array[attribute_matrix.col])))
    attribute_matrix = attribute_matrix.tocsr()

    return attribute_matrix


def query_list_of_words(target_word, list_of_words, edit_distance=1):
    """
    Checks whether a target word is within editing distance of any one in a set of keywords.

    Inputs: - target_word: A string containing the word we want to search in a list.
            - list_of_words: A python list of words.
            - edit_distance: For larger words, we also check for similar words based on edit_distance.

    Outputs: - new_list_of_words: This is the input list of words minus any found keywords.
             - found_list_of_words: This is the list of words that are within edit distance of the target word.
    """
    # Initialize lists
    new_list_of_words = list()
    found_list_of_words = list()

    append_left_keyword = new_list_of_words.append
    append_found_keyword = found_list_of_words.append

    # Iterate over the list of words
    for word in list_of_words:
        if len(word) > 6:
            effective_edit_distance = edit_distance
        else:
            effective_edit_distance = 0  # No edit distance for small words.
        if abs(len(word)-len(target_word)) <= effective_edit_distance:
            if nltk.edit_distance(word, target_word) <= effective_edit_distance:
                append_found_keyword(word)
            else:
                append_left_keyword(word)
        else:
            append_left_keyword(word)

    return new_list_of_words, found_list_of_words


def simple_word_query(target_word, list_of_words, edit_distance=1):
    found_list_of_words = list()
    append_found_keyword = found_list_of_words.append

    for word in list_of_words:
        if len(word) > 6:
            effective_edit_distance = edit_distance
        else:
            effective_edit_distance = 0  # No edit distance for small words.
        if abs(len(word)-len(target_word)) <= effective_edit_distance:
            if nltk.edit_distance(word, target_word) <= effective_edit_distance:
                append_found_keyword(word)
            else:
                pass
        else:
            pass

    return found_list_of_words
