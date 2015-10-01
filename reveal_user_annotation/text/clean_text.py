__author__ = 'Georgios Rizos (georgerizos@iti.gr)'

import os
import re
import string
import nltk
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
from nltk.stem.porter import PorterStemmer
from nltk.stem.snowball import SnowballStemmer
from nltk.stem.wordnet import WordNetLemmatizer
from multiprocessing import Pool
from functools import partial
from collections import defaultdict

from reveal_user_annotation.common.config_package import get_package_path, get_threads_number
from reveal_user_annotation.common.datarw import get_file_row_generator
from reveal_user_annotation.text.map_data import chunks
from reveal_user_annotation.text.text_util import reduce_list_of_bags_of_words, combine_word_list


def clean_single_word(word, lemmatizing="wordnet"):
    """
    Performs stemming or lemmatizing on a single word.

    If we are to search for a word in a clean bag-of-words, we need to search it after the same kind of preprocessing.

    Inputs: - word: A string containing the source word.
            - lemmatizing: A string containing one of the following: "porter", "snowball" or "wordnet".

    Output: - lemma: The resulting clean lemma or stem.
    """
    if lemmatizing == "porter":
        porter = PorterStemmer()
        lemma = porter.stem(word)
    elif lemmatizing == "snowball":
        snowball = SnowballStemmer('english')
        lemma = snowball.stem(word)
    elif lemmatizing == "wordnet":
        wordnet = WordNetLemmatizer()
        lemma = wordnet.lemmatize(word)
    else:
        print("Invalid lemmatizer argument.")
        raise RuntimeError

    return lemma


def clean_document(document, lemmatizing="wordnet"):
    """
    Extracts a clean bag-of-words from a document.

    Inputs: - document: A string containing some text.
            - lemmatizing: A string containing one of the following: "porter", "snowball" or "wordnet".

    Output: - lemma_list: A python list of lemmas or stems.
            - lemma_to_keywordbag: A python dictionary that maps stems/lemmas to original topic keywords.
    """
    ####################################################################################################################
    # Tokenizing text
    ####################################################################################################################
    try:
        tokenized_document = word_tokenize(document)
    except LookupError:
        print("Warning: Could not tokenize document. If these warnings are commonplace, there is a problem with the nltk resources.")
        lemma_list = list()
        lemma_to_keywordbag = defaultdict(lambda: defaultdict(int))
        return lemma_list, lemma_to_keywordbag

    ####################################################################################################################
    # Separate ["camelCase"] into ["camel", "case"] and make every letter lower case
    ####################################################################################################################
    tokenized_document = [separate_camel_case(token).lower() for token in tokenized_document]

    ####################################################################################################################
    # Parts of speech tagger
    ####################################################################################################################
    tokenized_document = nltk.pos_tag(tokenized_document)
    tokenized_document = [token[0] for token in tokenized_document if (token[1] == "JJ" or token[1] == "NN" or token[1] == "NNS" or token[1] == "NNP")]

    ####################################################################################################################
    # Removing digits, punctuation and whitespace
    ####################################################################################################################
    # See documentation here: http://docs.python.org/2/library/string.html
    regex = re.compile('[%s]' % re.escape(string.digits + string.punctuation + string.whitespace))

    tokenized_document_no_punctuation = list()
    append_token = tokenized_document_no_punctuation.append
    for token in tokenized_document:
        new_token = regex.sub(u'', token)
        if not new_token == u'':
            append_token(new_token)

    ####################################################################################################################
    # Removing stopwords
    ####################################################################################################################
    stopset = set(stopwords.words('english'))  # Make set for faster access

    more_stopword_files_list = os.listdir(get_package_path() + "/text/res/stopwords/")
    more_stopword_files_list = (get_package_path() + "/text/res/stopwords/" + file_name for file_name in more_stopword_files_list)

    # Read more stopwords from files
    extended_stopset = list()
    append_stopwords = extended_stopset.append
    for stop_word_file in more_stopword_files_list:
        file_row_gen = get_file_row_generator(stop_word_file, ",", encoding="utf-8")
        for row in file_row_gen:
            append_stopwords(row[0])
        stopset.update(extended_stopset)

    tokenized_document_no_stopwords = list()
    append_word = tokenized_document_no_stopwords.append
    for word in tokenized_document_no_punctuation:
        if word not in stopset:
            append_word(word)

    ####################################################################################################################
    # Remove words that have been created by automated list tools.
    ####################################################################################################################
    # # TODO: This should be done either for list keywords, or with a regex test(0-9), descr(0-9).
    # tokenized_document_no_stopwords_no_autowords = list()
    # append_word = tokenized_document_no_stopwords_no_autowords.append
    # for word in tokenized_document_no_stopwords:
    #     if not word.startswith(prefix=autoword_tuple):
    #         append_word(word)

    ####################################################################################################################
    # Stemming and Lemmatizing
    ####################################################################################################################
    lemma_to_keywordbag = defaultdict(lambda: defaultdict(int))

    final_doc = list()
    append_lemma = final_doc.append
    for word in tokenized_document_no_stopwords:
        if lemmatizing == "porter":
            porter = PorterStemmer()
            stem = porter.stem(word)
            append_lemma(stem)
            lemma_to_keywordbag[stem][word] += 1
        elif lemmatizing == "snowball":
            snowball = SnowballStemmer('english')
            stem = snowball.stem(word)
            append_lemma(stem)
            lemma_to_keywordbag[stem][word] += 1
        elif lemmatizing == "wordnet":
            wordnet = WordNetLemmatizer()
            lemma = wordnet.lemmatize(word)
            append_lemma(lemma)
            lemma_to_keywordbag[lemma][word] += 1
        else:
            print("Invalid lemmatizer argument.")
            raise RuntimeError

    ####################################################################################################################
    # One more stopword removal
    ####################################################################################################################
    lemma_list = list()
    append_word = lemma_list.append
    for word in final_doc:
        if word not in stopset:
            append_word(word)

    return lemma_list, lemma_to_keywordbag


def clean_corpus_serial(corpus, lemmatizing="wordnet"):
    """
    Extracts a bag-of-words from each document in a corpus serially.

    Inputs: - corpus: A python list of python strings. Each string is a document.
            - lemmatizing: A string containing one of the following: "porter", "snowball" or "wordnet".

    Output: - list_of_bags_of_words: A list of python dictionaries representing bags-of-words.
            - lemma_to_keywordbag_total: Aggregated python dictionary that maps stems/lemmas to original topic keywords.
    """
    list_of_bags_of_words = list()
    append_bag_of_words = list_of_bags_of_words.append

    lemma_to_keywordbag_total = defaultdict(lambda: defaultdict(int))

    for document in corpus:
        word_list, lemma_to_keywordbag = clean_document(document=document, lemmatizing=lemmatizing)
        bag_of_words = combine_word_list(word_list)
        append_bag_of_words(bag_of_words)

        for lemma, keywordbag in lemma_to_keywordbag.items():
            for keyword, multiplicity in keywordbag.items():
                lemma_to_keywordbag_total[lemma][keyword] += multiplicity

    return list_of_bags_of_words, lemma_to_keywordbag_total


def extract_bag_of_words_from_corpus_parallel(corpus, lemmatizing="wordnet"):
    """
    This extracts one bag-of-words from a list of strings. The documents are mapped to parallel processes.

    Inputs: - corpus: A list of strings.
            - lemmatizing: A string containing one of the following: "porter", "snowball" or "wordnet".

    Output: - bag_of_words: This is a bag-of-words in python dictionary format.
            - lemma_to_keywordbag_total: Aggregated python dictionary that maps stems/lemmas to original topic keywords.
    """
    ####################################################################################################################
    # Map and reduce document cleaning.
    ####################################################################################################################
    # Build a pool of processes.
    pool = Pool(processes=get_threads_number()*2,)

    # Partition the tweets to chunks.
    partitioned_corpus = chunks(corpus, len(corpus) / get_threads_number())

    # Map the cleaning of the tweet corpus to a pool of processes.
    list_of_bags_of_words, list_of_lemma_to_keywordset_maps = pool.map(partial(clean_corpus_serial, lemmatizing=lemmatizing), partitioned_corpus)

    # Reduce dictionaries to a single dictionary serially.
    bag_of_words = reduce_list_of_bags_of_words(list_of_bags_of_words)

    # Reduce lemma to keyword maps to a single dictionary.
    lemma_to_keywordbag_total = defaultdict(lambda: defaultdict(int))
    for lemma_to_keywordbag in list_of_lemma_to_keywordset_maps:
        for lemma, keywordbag in lemma_to_keywordbag.items():
            for keyword, multiplicity in keywordbag.items():
                lemma_to_keywordbag_total[lemma][keyword] += multiplicity

    return bag_of_words, lemma_to_keywordbag_total


first_cap_re = re.compile('(.)([A-Z][a-z]+)')
all_cap_re = re.compile('([a-z0-9])([A-Z])')
autoword_tuple = ("test", "descr")


def separate_camel_case(word):
    """
    What it says on the tin.

    Input:  - word: A string that may be in camelCase.

    Output: - separated_word: A list of strings with camel case separated.
    """
    s1 = first_cap_re.sub(r'\1 \2', word)
    separated_word = all_cap_re.sub(r'\1 \2', s1)
    return separated_word
