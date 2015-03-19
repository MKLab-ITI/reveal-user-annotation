__author__ = 'Georgios Rizos (georgerizos@iti.gr)'

from reveal_user_annotation.common.config_package import get_package_path


########################################################################################################################
# Setup configuration according to config.ini file.
########################################################################################################################
def configure_package():
    # Initialize configuration dictionary.
    configuration_dictionary = dict()
    configuration_dictionary["DATA_PATH"] = None
    configuration_dictionary["mongod_path"] = None
    configuration_dictionary["dbpath"] = None
    configuration_dictionary["PSERVER_HOST"] = None
    configuration_dictionary["ADMIN_PANEL_USER_NAME"] = None
    configuration_dictionary["ADMIN_PANEL_PASS"] = None
    configuration_dictionary["CLIENT_NAME"] = None
    configuration_dictionary["CLIENT_PASS"] = None
    configuration_dictionary["APP_KEY"] = None
    configuration_dictionary["APP_SECRET"] = None
    configuration_dictionary["SCREEN_NAME"] = None

    # Make configurations
    with open(get_package_path() + "/config.ini") as f:
        for file_row in f:
            if (file_row[0] != "#") and (file_row[0] is not None):
                words = file_row.strip().split("=")
                if len(words) > 1:
                    if words[1] == "None":
                        configuration_dictionary[words[0]] = None
                    else:
                        configuration_dictionary[words[0]] = words[1]

    # print("#################################")
    # print("Configure local disk data folder.")
    # print("#################################")
    configure_file(get_package_path() + "/common/res/config_data_path.txt",
                   configuration_dictionary,
                   "DATA_PATH",
                   "Local disk data folder")

    # print("########################")
    # print("Configure local MongoDB.")
    # print("########################")
    configure_file(get_package_path() + "/mongo/res/mongod_path.txt",
                   configuration_dictionary,
                   "mongod_path",
                   "Local mongo daemon executable path")

    configure_file(get_package_path() + "/mongo/res/mongodb.config",
                   configuration_dictionary,
                   "dbpath",
                   "Local mongo data folder")

    # print("##################")
    # print("Configure PServer.")
    # print("##################")
    with open(get_package_path() + "/pserver/res/pserver_config.txt", "w") as f:
        file_row = "PSERVER_HOST" + "=" + configuration_dictionary["PSERVER_HOST"] + "\n"
        f.write(file_row)
        # print(" - The PServer host address: ", configuration_dictionary["PSERVER_HOST"])

        file_row = "ADMIN_PANEL_USER_NAME" + "=" + configuration_dictionary["ADMIN_PANEL_USER_NAME"] + "\n"
        f.write(file_row)
        # print(" - The PServer administration panel user name: ", configuration_dictionary["ADMIN_PANEL_USER_NAME"])

        file_row = "ADMIN_PANEL_PASS" + "=" + configuration_dictionary["ADMIN_PANEL_PASS"] + "\n"
        f.write(file_row)
        # print(" - The PServer administration panel password: ", configuration_dictionary["ADMIN_PANEL_PASS"])

        file_row = "CLIENT_NAME" + "=" + configuration_dictionary["CLIENT_NAME"] + "\n"
        f.write(file_row)
        # print(" - A PServer client name: ", configuration_dictionary["CLIENT_NAME"])

        file_row = "CLIENT_PASS" + "=" + configuration_dictionary["CLIENT_PASS"] + "\n"
        f.write(file_row)
        # print(" - A PServer client password: ", configuration_dictionary["CLIENT_PASS"])

    # print("##################")
    # print("Configure Twitter.")
    # print("##################")
    with open(get_package_path() + "/twitter/res/oauth_login/config_app_credentials.txt", "w") as f:
        file_row = "APP_KEY" + "=" + configuration_dictionary["APP_KEY"] + "\n"
        f.write(file_row)
        # print(" - The Twitter app key: ", configuration_dictionary["APP_KEY"])

        file_row = "APP_SECRET" + "=" + configuration_dictionary["APP_SECRET"] + "\n"
        f.write(file_row)
        # print(" - The Twitter app secret: ", configuration_dictionary["APP_SECRET"])

        file_row = "SCREEN_NAME" + "=" + configuration_dictionary["SCREEN_NAME"] + "\n"
        f.write(file_row)
        # print(" - The Twitter developer user screen name: ", configuration_dictionary["SCREEN_NAME"])


def configure_file(file_path, configuration_dictionary, key, print_string):
    with open(file_path, "w") as f:
        file_row = key + "=" + configuration_dictionary[key] + "\n"
        f.write(file_row)
        # print(" - " + print_string + ": ", configuration_dictionary[key])
