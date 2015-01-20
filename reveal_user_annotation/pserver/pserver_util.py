__author__ = 'Georgios Rizos (georgerizos@iti.gr)'

from reveal_user_annotation.common.config_package import get_package_path
from reveal_user_annotation.common.datarw import get_file_row_generator


def get_configuration_details():
    pserver_config_file_path = get_package_path() + "/pserver/res/pserver_config.txt"

    file_row_gen = get_file_row_generator(pserver_config_file_path, "=")

    configuration_dictionary = dict()
    for file_row in file_row_gen:
        configuration_dictionary[file_row[0]] = file_row[1]

    return configuration_dictionary