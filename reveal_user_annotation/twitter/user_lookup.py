__author__ = 'Georgios Rizos (georgerizos@iti.gr)'

import time
import twython
from urllib.error import URLError
from http.client import BadStatusLine

from reveal_user_annotation.text.map_data import chunks
from reveal_user_annotation.twitter.twitter_util import login, safe_twitter_request_handler


def check_suspension(user_twitter_id_list):
    """
    Looks up a list of user ids and checks whether they are currently suspended.

    Input: - user_twitter_id_list: A python list of Twitter user ids in integer format to be looked-up.

    Outputs: - suspended_user_twitter_id_list: A python list of suspended Twitter user ids in integer format.
             - non_suspended_user_twitter_id_list: A python list of non suspended Twitter user ids in integer format.
             - unknown_status_user_twitter_id_list: A python list of unknown status Twitter user ids in integer format.
    """
    ####################################################################################################################
    # Log into my application.
    ####################################################################################################################
    twitter = login()

    ####################################################################################################################
    # Lookup users
    ####################################################################################################################
    # Initialize look-up lists
    suspended_user_twitter_id_list = list()
    non_suspended_user_twitter_id_list = list()
    unknown_status_user_twitter_id_list = list()

    append_suspended_twitter_user = suspended_user_twitter_id_list.append
    append_non_suspended_twitter_user = non_suspended_user_twitter_id_list.append
    extend_unknown_status_twitter_user = unknown_status_user_twitter_id_list.extend

    # Split twitter user id list into sub-lists of length 100 (This is the Twitter API function limit).
    user_lookup_counter = 0
    user_lookup_time_window_start = time.perf_counter()
    for hundred_length_sub_list in chunks(list(user_twitter_id_list), 100):
        # Make safe twitter request.
        try:
            api_result, user_lookup_counter, user_lookup_time_window_start\
                = safe_twitter_request_handler(twitter_api_func=twitter.lookup_user,
                                               call_rate_limit=60,
                                               call_counter=user_lookup_counter,
                                               time_window_start=user_lookup_time_window_start,
                                               max_retries=10,
                                               wait_period=2,
                                               parameters=hundred_length_sub_list)
            # If the call is succesful, turn hundred sub-list to a set for faster search.
            hundred_length_sub_list = set(hundred_length_sub_list)

            # Check who is suspended and who is not.
            for hydrated_user_object in api_result:
                hydrated_twitter_user_id = hydrated_user_object["id"]

                if hydrated_twitter_user_id in hundred_length_sub_list:
                    append_non_suspended_twitter_user(hydrated_twitter_user_id)
                else:
                    append_suspended_twitter_user(hydrated_twitter_user_id)
        except twython.TwythonError:
            # If the call is unsuccesful, we do not know about the status of the users.
            extend_unknown_status_twitter_user(hundred_length_sub_list)
        except URLError:
            # If the call is unsuccesful, we do not know about the status of the users.
            extend_unknown_status_twitter_user(hundred_length_sub_list)
        except BadStatusLine:
            # If the call is unsuccesful, we do not know about the status of the users.
            extend_unknown_status_twitter_user(hundred_length_sub_list)

    return suspended_user_twitter_id_list, non_suspended_user_twitter_id_list, unknown_status_user_twitter_id_list
