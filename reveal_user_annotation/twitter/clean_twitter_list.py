__author__ = 'Georgios Rizos (georgerizos@iti.gr)'

from collections import defaultdict

from reveal_user_annotation.text.clean_text import clean_document
from reveal_user_annotation.text.text_util import reduce_list_of_bags_of_words


def clean_twitter_list(twitter_list, lemmatizing="wordnet"):
    """
    Extracts the *set* of keywords found in a Twitter list (name + description).

    Inputs: - twitter_list: A Twitter list in json format.
            - lemmatizing: A string containing one of the following: "porter", "snowball" or "wordnet".

    Output: - keyword_set: A set of keywords (i.e. not a bag-of-words) in python set format.
            - lemma_to_keywordbag: A python dictionary that maps stems/lemmas to original topic keywords.
    """
    name_lemmas, name_lemma_to_keywordbag = clean_document(twitter_list["name"].replace("_", " ").replace("-", " "),
                                                           lemmatizing=lemmatizing)
    description_lemmas, description_lemma_to_keywordbag = clean_document(twitter_list["description"].replace("_", " ").replace("-", " "),
                                                                         lemmatizing=lemmatizing)

    keyword_set = set(name_lemmas + description_lemmas)

    lemma_to_keywordbag = defaultdict(lambda: defaultdict(int))
    for lemma, keywordbag in name_lemma_to_keywordbag.items():
        for keyword, multiplicity in keywordbag.items():
            lemma_to_keywordbag[lemma][keyword] += multiplicity

    for lemma, keywordbag in description_lemma_to_keywordbag.items():
        for keyword, multiplicity in keywordbag.items():
            lemma_to_keywordbag[lemma][keyword] += multiplicity

    return keyword_set, lemma_to_keywordbag


def clean_list_of_twitter_list(list_of_twitter_lists, lemmatizing="wordnet"):
    """
    Extracts the sets of keywords for each Twitter list.

    Inputs: - list_of_twitter_lists: A python list of Twitter lists in json format.
            - lemmatizing: A string containing one of the following: "porter", "snowball" or "wordnet".

    Output: - list_of_keyword_sets: A list of sets of keywords (i.e. not a bag-of-words) in python set format.
            - list_of_lemma_to_keywordbags: List of python dicts that map stems/lemmas to original topic keywords.
    """
    list_of_keyword_sets = list()
    append_keyword_set = list_of_keyword_sets.append

    list_of_lemma_to_keywordbags = list()
    append_lemma_to_keywordbag = list_of_lemma_to_keywordbags.append

    if list_of_twitter_lists is not None:
        for twitter_list in list_of_twitter_lists:
            if twitter_list is not None:
                keyword_set, lemma_to_keywordbag = clean_twitter_list(twitter_list=twitter_list, lemmatizing=lemmatizing)
                append_keyword_set(keyword_set)
                append_lemma_to_keywordbag(lemma_to_keywordbag)

    return list_of_keyword_sets, list_of_lemma_to_keywordbags


def user_twitter_list_bag_of_words(twitter_list_corpus, lemmatizing="wordnet"):
    """
    Extract a bag-of-words for a corpus of Twitter lists pertaining to a Twitter user.

    Inputs: - twitter_list_corpus: A python list of Twitter lists in json format.
            - lemmatizing: A string containing one of the following: "porter", "snowball" or "wordnet".

    Output: - bag_of_words: A bag-of-words in python dictionary format.
            - lemma_to_keywordbag_total: Aggregated python dictionary that maps stems/lemmas to original topic keywords.
    """
    # Extract a bag-of-words from a list of Twitter lists.
    # May result in empty sets
    list_of_keyword_sets, list_of_lemma_to_keywordbags = clean_list_of_twitter_list(twitter_list_corpus, lemmatizing)

    # Reduce keyword sets.
    bag_of_words = reduce_list_of_bags_of_words(list_of_keyword_sets)

    # Reduce lemma to keywordbag maps.
    lemma_to_keywordbag_total = defaultdict(lambda: defaultdict(int))
    for lemma_to_keywordbag in list_of_lemma_to_keywordbags:
        for lemma, keywordbag in lemma_to_keywordbag.items():
            for keyword, multiplicity in keywordbag.items():
                lemma_to_keywordbag_total[lemma][keyword] += multiplicity

    return bag_of_words, lemma_to_keywordbag_total
