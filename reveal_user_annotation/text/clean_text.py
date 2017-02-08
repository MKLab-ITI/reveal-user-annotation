__author__ = 'Georgios Rizos (georgerizos@iti.gr)'

import os
import re
import string

from nltk.corpus import stopwords
from nltk.stem.porter import PorterStemmer
from nltk.stem.snowball import SnowballStemmer
from nltk.stem.wordnet import WordNetLemmatizer
from nltk.tag import brill
from nltk.tag.brill_trainer import BrillTaggerTrainer
import nltk
from multiprocessing import Pool
from functools import partial
from collections import defaultdict

from reveal_user_annotation.common.config_package import get_package_path, get_threads_number
from reveal_user_annotation.common.datarw import get_file_row_generator
from reveal_user_annotation.text.map_data import chunks
from reveal_user_annotation.text.text_util import reduce_list_of_bags_of_words, combine_word_list


def backoff_tagger(tagged_sents, tagger_classes, backoff=None):
    if not backoff:
        backoff = tagger_classes[0](tagged_sents)
        del tagger_classes[0]

    for cls in tagger_classes:
        tagger = cls(tagged_sents, backoff=backoff)
        backoff = tagger

    return backoff


def get_word_patterns():
    word_patterns = [
    (r'^-?[0-9]+(.[0-9]+)?$', 'CD'),
    (r'.*ould$', 'MD'),
    (r'.*ing$', 'VBG'),
    (r'.*ed$', 'VBD'),
    (r'.*ness$', 'NN'),
    (r'.*ment$', 'NN'),
    (r'.*ful$', 'JJ'),
    (r'.*ious$', 'JJ'),
    (r'.*ble$', 'JJ'),
    (r'.*ic$', 'JJ'),
    (r'.*ive$', 'JJ'),
    (r'.*ic$', 'JJ'),
    (r'.*est$', 'JJ'),
    (r'^a$', 'PREP'),
    ]
    return word_patterns


def get_braupt_tagger():
    conll_sents = nltk.corpus.conll2000.tagged_sents()
    # conll_sents = nltk.corpus.conll2002.tagged_sents()

    word_patterns = get_word_patterns()
    raubt_tagger = backoff_tagger(conll_sents, [nltk.tag.AffixTagger,
    nltk.tag.UnigramTagger, nltk.tag.BigramTagger, nltk.tag.TrigramTagger],
    backoff=nltk.tag.RegexpTagger(word_patterns))

    templates = brill.brill24()

    trainer = BrillTaggerTrainer(raubt_tagger, templates)
    braubt_tagger = trainer.train(conll_sents, max_rules=100, min_score=3)
    return braubt_tagger


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


def clean_document(document,
                   sent_tokenize, _treebank_word_tokenize,
                   tagger,
                   lemmatizer,
                   lemmatize,
                   stopset,
                   first_cap_re, all_cap_re,
                   digits_punctuation_whitespace_re,
                   pos_set):
    """
    Extracts a clean bag-of-words from a document.

    Inputs: - document: A string containing some text.

    Output: - lemma_list: A python list of lemmas or stems.
            - lemma_to_keywordbag: A python dictionary that maps stems/lemmas to original topic keywords.
    """
    ####################################################################################################################
    # Tokenizing text
    ####################################################################################################################
    # start_time = time.perf_counter()
    try:
        tokenized_document = fast_word_tokenize(document, sent_tokenize, _treebank_word_tokenize)
    except LookupError:
        print("Warning: Could not tokenize document. If these warnings are commonplace, there is a problem with the nltk resources.")
        lemma_list = list()
        lemma_to_keywordbag = defaultdict(lambda: defaultdict(int))
        return lemma_list, lemma_to_keywordbag
    # elapsed_time = time.perf_counter() - start_time
    # print("Tokenize", elapsed_time)

    ####################################################################################################################
    # Separate ["camelCase"] into ["camel", "case"] and make every letter lower case
    ####################################################################################################################
    # start_time = time.perf_counter()
    tokenized_document = [separate_camel_case(token, first_cap_re, all_cap_re).lower() for token in tokenized_document]
    # elapsed_time = time.perf_counter() - start_time
    # print("camelCase", elapsed_time)

    ####################################################################################################################
    # Parts of speech tagger
    ####################################################################################################################
    # start_time = time.perf_counter()
    tokenized_document = tagger.tag(tokenized_document)
    tokenized_document = [token[0] for token in tokenized_document if (token[1] in pos_set)]
    # elapsed_time = time.perf_counter() - start_time
    # print("POS", elapsed_time)

    ####################################################################################################################
    # Removing digits, punctuation and whitespace
    ####################################################################################################################
    # start_time = time.perf_counter()
    tokenized_document_no_punctuation = list()
    append_token = tokenized_document_no_punctuation.append
    for token in tokenized_document:
        new_token = remove_digits_punctuation_whitespace(token, digits_punctuation_whitespace_re)
        if not new_token == u'':
            append_token(new_token)
    # elapsed_time = time.perf_counter() - start_time
    # print("digits etc", elapsed_time)

    ####################################################################################################################
    # Removing stopwords
    ####################################################################################################################
    # start_time = time.perf_counter()
    tokenized_document_no_stopwords = list()
    append_word = tokenized_document_no_stopwords.append
    for word in tokenized_document_no_punctuation:
        if word not in stopset:
            append_word(word)
    # elapsed_time = time.perf_counter() - start_time
    # print("stopwords 1", elapsed_time)

    ####################################################################################################################
    # Stemming and Lemmatizing
    ####################################################################################################################
    # start_time = time.perf_counter()
    lemma_to_keywordbag = defaultdict(lambda: defaultdict(int))

    final_doc = list()
    append_lemma = final_doc.append
    for word in tokenized_document_no_stopwords:
        lemma = lemmatize(word)
        append_lemma(lemma)
        lemma_to_keywordbag[lemma][word] += 1
    # elapsed_time = time.perf_counter() - start_time
    # print("lemmatize", elapsed_time)

    ####################################################################################################################
    # One more stopword removal
    ####################################################################################################################
    # start_time = time.perf_counter()
    lemma_list = list()
    append_word = lemma_list.append
    for word in final_doc:
        if word not in stopset:
            append_word(word)
    # elapsed_time = time.perf_counter() - start_time
    # print("stopwords 2", elapsed_time)

    return lemma_list, lemma_to_keywordbag


def get_tokenizer():
    sent_tokenize = nltk.tokenize.sent_tokenize
    _treebank_word_tokenize = nltk.tokenize._treebank_word_tokenize

    return sent_tokenize, _treebank_word_tokenize


def fast_word_tokenize(text, sent_tokenize, _treebank_word_tokenize, language='english'):
    return [token for sent in sent_tokenize(text, language)
            for token in _treebank_word_tokenize(sent)]


def get_stopset():
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
    return stopset


def get_lemmatizer(lemmatizing="wordnet"):
    if lemmatizing == "porter":
        lemmatizer = PorterStemmer()
        lemmatize = lemmatizer.stem
    elif lemmatizing == "snowball":
        lemmatizer = SnowballStemmer('english')
        lemmatize = lemmatizer.stem
    elif lemmatizing == "wordnet":
        lemmatizer = WordNetLemmatizer()
        lemmatize = lemmatizer.lemmatize
    else:
        print("Invalid lemmatizer argument.")
        raise RuntimeError
    return lemmatizer, lemmatize


def get_pos_set():
    pos_set = set(["JJ", "NN", "NNS", "NNP"])
    return pos_set


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
        word_list, lemma_to_keywordbag = clean_document(document=document, lemmatizing=lemmatizing)  # TODO: Alter this.
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


autoword_tuple = ("test", "descr")


def get_camel_case_regexes():
    first_cap_re = re.compile('(.)([A-Z][a-z]+)')
    all_cap_re = re.compile('([a-z0-9])([A-Z])')

    return first_cap_re, all_cap_re


def separate_camel_case(word, first_cap_re, all_cap_re):
    """
    What it says on the tin.

    Input:  - word: A string that may be in camelCase.

    Output: - separated_word: A list of strings with camel case separated.
    """
    s1 = first_cap_re.sub(r'\1 \2', word)
    separated_word = all_cap_re.sub(r'\1 \2', s1)
    return separated_word


def get_digits_punctuation_whitespace_regex():
    # See documentation here: http://docs.python.org/2/library/string.html
    digits_punctuation_whitespace_re = re.compile('[%s]' % re.escape(string.digits + string.punctuation + string.whitespace))
    return digits_punctuation_whitespace_re


def remove_digits_punctuation_whitespace(word, digits_punctuation_whitespace_re):
    new_word = digits_punctuation_whitespace_re.sub(u'', word)
    return new_word
