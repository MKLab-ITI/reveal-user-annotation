__author__ = 'Georgios Rizos (georgerizos@iti.gr)'

from reveal_user_annotation.common.config_package import get_package_path
from reveal_user_annotation.common.datarw import get_file_row_generator


def get_topic_set(file_path):
    """
    Opens one of the topic set resource files and returns a set of topics.

    - Input:  - file_path: The path pointing to the topic set resource file.

    - Output: - topic_set: A python set of strings.
    """
    topic_set = set()
    file_row_gen = get_file_row_generator(file_path, ",")  # The separator here is irrelevant.
    for file_row in file_row_gen:
        topic_set.add(file_row[0])

    return topic_set


def get_story_set():
    """
    Returns a set of all the story-related topics found in the SNOW dataset.
    """
    file_path = get_package_path() + "/twitter/res/topics/story_set.txt"
    topics = get_topic_set(file_path)

    return topics


def get_theme_set():
    """
    Returns a set of all the thematic-related keywords found in the SNOW dataset.
    """
    file_path = get_package_path() + "/twitter/res/topics/theme_set.txt"
    topics = get_topic_set(file_path)

    return topics


def get_attribute_set():
    """
    Returns a set of all the user-related attributes (e.g. occupation) found in the SNOW dataset.
    """
    file_path = get_package_path() + "/twitter/res/topics/attribute_set.txt"
    topics = get_topic_set(file_path)

    return topics


def get_stance_set():
    """
    Returns a set of all the user-related stances (e.g. political affiliations, religious view) found in the SNOW dataset.
    """
    file_path = get_package_path() + "/twitter/res/topics/stance_set.txt"
    topics = get_topic_set(file_path)

    return topics


def get_geographical_set():
    """
    Returns a set of all the geographical attributes (e.g. uk, sweden) found in the SNOW dataset.
    """
    file_path = get_package_path() + "/twitter/res/topics/geographical_set.txt"
    topics = get_topic_set(file_path)

    return topics


def get_sport_set():
    """
    Returns a set of all the sport-related attributes (e.g. football, golf) found in the SNOW dataset.
    """
    file_path = get_package_path() + "/twitter/res/topics/sport_set.txt"
    topics = get_topic_set(file_path)

    return topics


def get_reveal_set():
    """
    Returns a set of all the topics that are interesting for REVEAL use-cases.
    """
    file_path = get_package_path() + "/twitter/res/topics/story_set.txt"
    story_topics = get_topic_set(file_path)

    file_path = get_package_path() + "/twitter/res/topics/theme_set.txt"
    theme_topics = get_topic_set(file_path)

    file_path = get_package_path() + "/twitter/res/topics/attribute_set.txt"
    attribute_topics = get_topic_set(file_path)

    file_path = get_package_path() + "/twitter/res/topics/stance_set.txt"
    stance_topics = get_topic_set(file_path)

    file_path = get_package_path() + "/twitter/res/topics/geographical_set.txt"
    geographical_topics = get_topic_set(file_path)

    topics = story_topics | theme_topics | attribute_topics | stance_topics | geographical_topics

    return topics


def get_topic_keyword_dictionary():
    """
    Opens the topic-keyword map resource file and returns the corresponding python dictionary.

    - Input:  - file_path: The path pointing to the topic-keyword map resource file.

    - Output: - topic_set: A topic to keyword python dictionary.
    """
    topic_keyword_dictionary = dict()
    file_row_gen = get_file_row_generator(get_package_path() + "/twitter/res/topics/topic_keyword_mapping" + ".txt",
                                          ",",
                                          "utf-8")
    for file_row in file_row_gen:
        topic_keyword_dictionary[file_row[0]] = set([keyword for keyword in file_row[1:]])

    return topic_keyword_dictionary
