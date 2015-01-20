__author__ = 'Georgios Rizos (georgerizos@iti.gr)'

import os
import inspect
import multiprocessing

import reveal_user_annotation
from reveal_user_annotation.common.datarw import get_file_row_generator


########################################################################################################################
# Configure path related functions.
########################################################################################################################
def get_package_path():
    return os.path.dirname(inspect.getfile(reveal_user_annotation))


def get_data_path():
    data_file_path = get_package_path() + "/common/res/config_data_path.txt"
    file_row_gen = get_file_row_generator(data_file_path, "=")
    file_row = next(file_row_gen)
    data_path = file_row[1]
    return data_path


def get_raw_datasets_path():
    return get_data_path() + "/raw_data"


def get_memory_path():
    return get_data_path() + "/memory"


########################################################################################################################
# Configure optimization related functions.
########################################################################################################################
def get_threads_number():
    """
    Automatically determine the number of cores. If that fails, the number defaults to a manual setting.
    """
    try:
        cores_number = multiprocessing.cpu_count()
        return cores_number
    except NotImplementedError:
        cores_number = 8
        return cores_number
