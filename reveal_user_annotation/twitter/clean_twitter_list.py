__author__ = 'Georgios Rizos (georgerizos@iti.gr)'

from reveal_user_annotation.text.clean_text import clean_document
from reveal_user_annotation.text.text_util import reduce_list_of_bags_of_words


def clean_twitter_list(twitter_list, lemmatizing="wordnet"):
    """
    Extracts the *set* of keywords found in a Twitter list (name + description).

    Inputs: - twitter_list: A Twitter list in json format.
            - lemmatizing: A string containing one of the following: "porter", "snowball" or "wordnet".

    Output: - keyword_set: A set of keywords (i.e. not a bag-of-words) in python set format.
    """
    name_lemmas = clean_document(twitter_list["name"].replace("_", " ").replace("-", " "),
                                 lemmatizing=lemmatizing)
    description_lemmas = clean_document(twitter_list["description"].replace("_", " ").replace("-", " "),
                                        lemmatizing=lemmatizing)

    keyword_set = set(name_lemmas + description_lemmas)

    return keyword_set


def clean_list_of_twitter_list(list_of_twitter_lists, lemmatizing="wordnet"):
    """
    Extracts the sets of keywords for each Twitter list.

    Inputs: - list_of_twitter_lists: A python list of Twitter lists in json format.
            - lemmatizing: A string containing one of the following: "porter", "snowball" or "wordnet".

    Output: - list_of_keyword_sets: A list of sets of keywords (i.e. not a bag-of-words) in python set format.
    """
    list_of_keyword_sets = [clean_twitter_list(twitter_list=twitter_list, lemmatizing=lemmatizing) for twitter_list in list_of_twitter_lists if twitter_list is not None]

    return list_of_keyword_sets


def user_twitter_list_bag_of_words(twitter_list_corpus, lemmatizing="wordnet"):
    """
    Extract a bag-of-words for a corpus of Twitter lists pertaining to a Twitter user.

    Inputs: - twitter_list_corpus: A python list of Twitter lists in json format.
            - lemmatizing: A string containing one of the following: "porter", "snowball" or "wordnet".

    Output: - bag_of_words: A bag-of-words in python dictionary format.
    """
    # Extract a bag-of-words from a list of Twitter lists
    # May result in empty sets
    list_of_keyword_sets = clean_list_of_twitter_list(twitter_list_corpus, lemmatizing)

    # Reduce keyword sets
    bag_of_words = reduce_list_of_bags_of_words(list_of_keyword_sets)

    return bag_of_words
