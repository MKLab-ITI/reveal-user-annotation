__author__ = 'Georgios Rizos (georgerizos@iti.gr)'

from reveal_user_annotation.common.config_package import get_package_path
from reveal_user_annotation.common.datarw import get_file_row_generator


def get_configuration_details():
    pserver_config_file_path = get_package_path() + "/pserver/res/pserver_config.txt"

    file_row_gen = get_file_row_generator(pserver_config_file_path, "=")

    configuration_dictionary = dict()
    for file_row in file_row_gen:
        configuration_dictionary[file_row[0]] = file_row[1]

    pserver_host = configuration_dictionary["PSERVER_HOST"]
    admin_panel_user_name = configuration_dictionary["ADMIN_PANEL_USER_NAME"]
    admin_panel_pass = configuration_dictionary["ADMIN_PANEL_PASS"]
    client_name = configuration_dictionary["CLIENT_NAME"]
    client_pass = configuration_dictionary["CLIENT_PASS"]

    return pserver_host, admin_panel_user_name, admin_panel_pass, client_name, client_pass