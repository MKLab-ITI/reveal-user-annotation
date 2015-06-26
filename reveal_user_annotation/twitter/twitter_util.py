__author__ = 'Georgios Rizos (georgerizos@iti.gr)'

import twython
from twython import Twython
import time
from urllib.error import URLError
from http.client import BadStatusLine

from reveal_user_annotation.common.config_package import get_package_path
from reveal_user_annotation.common.datarw import get_file_row_generator


def login(twitter_app_key,
          twitter_app_secret):
    """
    This is a shortcut for logging in the Twitter app described in the config_app_credentials.txt file.

    Output: - twitter: A twython twitter object, containing wrappers for the Twitter API.
    """
    # Log into my application
    twitter = Twython(twitter_app_key, twitter_app_secret)
    # auth = twitter.get_authentication_tokens(screen_name=screen_name)

    return twitter


def safe_twitter_request_handler(twitter_api_func,
                                 call_rate_limit,
                                 call_counter,
                                 time_window_start,
                                 max_retries,
                                 wait_period,
                                 *args, **kw):
    """
    This is a safe function handler for any twitter request.

    Inputs:  - twitter_api_func: The twython function object to be safely called.
             - call_rate_limit: THe call rate limit for this specific Twitter API function.
             - call_counter: A counter that keeps track of the number of function calls in the current 15-minute window.
             - time_window_start: The timestamp of the current 15-minute window.
             - max_retries: Number of call retries allowed before abandoning the effort.
             - wait_period: For certain Twitter errors (i.e. server overload), we wait and call again.
             - *args, **kw: The parameters of the twython function to be called.

    Outputs: - twitter_api_function_result: The results of the Twitter function.
             - call_counter: A counter that keeps track of the number of function calls in the current 15-minute window.
             - time_window_start: The timestamp of the current 15-minute window.

    Raises: - twython.TwythonError
            - urllib.error.URLError
            - http.client.BadStatusLine
    """
    error_count = 0

    while True:
        try:
            # If we have reached the call rate limit for this function:
            if call_counter >= call_rate_limit:
                # Reset counter.
                call_counter = 0

                # Sleep for the appropriate time.
                elapsed_time = time.perf_counter() - time_window_start
                sleep_time = 15*60 - elapsed_time
                if sleep_time < 0.1:
                    sleep_time = 0.1
                time.sleep(sleep_time)

                # Initialize new 15-minute time window.
                time_window_start = time.perf_counter()
            else:
                call_counter += 1
                twitter_api_function_result = twitter_api_func(*args, **kw)
                return twitter_api_function_result, call_counter, time_window_start
        except twython.TwythonError as e:
            # If it is a Twitter error, handle it.
            error_count, call_counter, time_window_start, wait_period = handle_twitter_http_error(e,
                                                                                                  error_count,
                                                                                                  call_counter,
                                                                                                  time_window_start,
                                                                                                  wait_period)
            if error_count > max_retries:
                print("Max error count reached. Abandoning effort.")
                raise e
        except URLError as e:
            error_count += 1
            if error_count > max_retries:
                print("Max error count reached. Abandoning effort.")
                raise e
        except BadStatusLine as e:
            error_count += 1
            if error_count > max_retries:
                print("Max error count reached. Abandoning effort.")
                raise e


def handle_twitter_http_error(e, error_count, call_counter, time_window_start, wait_period):
    """
    This function handles the twitter request in case of an HTTP error.

    Inputs:  - e: A twython.TwythonError instance to be handled.
             - error_count: Number of failed retries of the call until now.
             - call_counter: A counter that keeps track of the number of function calls in the current 15-minute window.
             - time_window_start: The timestamp of the current 15-minute window.
             - wait_period: For certain Twitter errors (i.e. server overload), we wait and call again.

    Outputs: - call_counter: A counter that keeps track of the number of function calls in the current 15-minute window.
             - time_window_start: The timestamp of the current 15-minute window.
             - wait_period: For certain Twitter errors (i.e. server overload), we wait and call again.

    Raises: - twython.TwythonError
    """
    if e.error_code == 401:
        # Encountered 401 Error (Not Authorized)
        raise e
    elif e.error_code == 404:
        # Encountered 404 Error (Not Found)
        raise e
    elif e.error_code == 429:
        # Encountered 429 Error (Rate Limit Exceeded)
        # Sleep for 15 minutes
        error_count += 0.5
        call_counter = 0
        wait_period = 2
        time.sleep(60*15 + 5)
        time_window_start = time.perf_counter()
        return error_count, call_counter, time_window_start, wait_period
    elif e.error_code in (500, 502, 503, 504):
        error_count += 1
        time.sleep(wait_period)
        wait_period *= 1.5
        return error_count, call_counter, time_window_start, wait_period
    else:
        raise e
