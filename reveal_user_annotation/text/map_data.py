__author__ = 'Georgios Rizos (georgerizos@iti.gr)'

from itertools import islice, zip_longest
import numpy as np


def grouper(iterable, n, pad_value=None):
    """
    Returns a generator of n-length chunks of an input iterable, with appropriate padding at the end.

    Example: grouper(3, 'abcdefg', 'x') --> ('a','b','c'), ('d','e','f'), ('g','x','x')

    Inputs: - iterable: The source iterable that needs to be chunkified.
            - n: The size of the chunks.
            - pad_value: The value with which the last chunk will be padded.

    Output: - chunk_gen: A generator of n-length chunks of an input iterable.
    """
    chunk_gen = (chunk for chunk in zip_longest(*[iter(iterable)]*n, fillvalue=pad_value))
    return chunk_gen


def chunks(iterable, n):
    """
    A python generator that yields 100-length sub-list chunks.

    Input:  - full_list: The input list that is to be separated in chunks of 100.
            - chunk_size: Should be set to 100, unless the Twitter API changes.

    Yields: - sub_list: List chunks of length 100.
    """
    for i in np.arange(0, len(iterable), n):
        yield iterable[i:i+n]


def split_every(iterable, n):  # TODO: Remove this, or make it return a generator.
    """
    A generator of n-length chunks of an input iterable
    """
    i = iter(iterable)
    piece = list(islice(i, n))
    while piece:
        yield piece
        piece = list(islice(i, n))
