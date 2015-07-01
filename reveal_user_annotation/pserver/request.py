__author__ = 'Georgios Rizos (georgerizos@iti.gr)'

import requests
import xml.etree.cElementTree as etree
from io import StringIO


def get_user_list(host_name, client_name, client_pass):
    """
    Pulls the list of users in a client.

    Inputs: - host_name: A string containing the address of the machine where the PServer instance is hosted.
            - client_name: The PServer client name.
            - client_pass: The PServer client's password.

    Output: - user_id_list: A python list of user ids.

    Raises: -
    """
    # Construct request.
    request = construct_request(model_type="pers",
                                client_name=client_name,
                                client_pass=client_pass,
                                command="getusrs",
                                values="whr=*")

    # Make request.
    request_result = send_request(host_name, request)

    # Extract a python list from xml object.
    user_id_list = list()
    append_user_id = user_id_list.append

    if request_result is not None:
        user_list_xml = request_result.text
        tree = etree.parse(StringIO(user_list_xml))
        root = tree.getroot()

        xml_rows = root.findall("./result/row/usr")
        for xml_row in xml_rows:
            append_user_id(xml_row.text)

    return user_id_list


def add_features(host_name, client_name, client_pass, feature_names):
    """
    Add a number of numerical features in the client.

    Inputs: - host_name: A string containing the address of the machine where the PServer instance is hosted.
            - client_name: The PServer client name.
            - client_pass: The PServer client's password.
            - feature_names: A python list of feature names.
    """
    init_feats = ("&".join(["%s=0"]*len(feature_names))) % tuple(feature_names)
    features_req = construct_request("pers",
                                     client_name,
                                     client_pass,
                                     "addftr",
                                     init_feats)
    send_request(host_name,
                 features_req)


def delete_features(host_name, client_name, client_pass, feature_names=None):
    """
    Remove a number of numerical features in the client. If a list  is not provided, remove all features.

    Inputs: - host_name: A string containing the address of the machine where the PServer instance is hosted.
            - client_name: The PServer client name.
            - client_pass: The PServer client's password.
            - feature_names: A python list of feature names.
    """
    # Get all features.
    if feature_names is None:
        feature_names = get_feature_names(host_name,
                                          client_name,
                                          client_pass)

    # Remove all features.
    feature_to_be_removed = ("&".join(["ftr=%s"]*len(feature_names))) % tuple(feature_names)
    features_req = construct_request("pers",
                                     client_name,
                                     client_pass,
                                     'remftr',
                                     feature_to_be_removed)
    send_request(host_name,
                 features_req)


def get_feature_names(host_name, client_name, client_pass):
    """
    Get the names of all features in a PServer client.

    Inputs: - host_name: A string containing the address of the machine where the PServer instance is hosted.
            - client_name: The PServer client name.
            - client_pass: The PServer client's password.

    Output: - feature_names: A python list of feature names.
    """
    # Construct request.
    request = construct_request(model_type="pers",
                                client_name=client_name,
                                client_pass=client_pass,
                                command="getftrdef",
                                values="ftr=*")

    # Send request.
    request_result = send_request(host_name,
                                  request)

    # Extract a python list from xml object.
    feature_names = list()
    append_feature_name = feature_names.append

    if request_result is not None:
        feature_names_xml = request_result.text
        tree = etree.parse(StringIO(feature_names_xml))
        root = tree.getroot()

        xml_rows = root.findall("row/ftr")
        for xml_row in xml_rows:
            append_feature_name(xml_row.text)

    return feature_names


def insert_user_data(host_name, client_name, client_pass, user_twitter_id, topic_to_score):
    """
    Inserts topic/score data for a user to a PServer client.

    Inputs: - host_name: A string containing the address of the machine where the PServer instance is hosted.
            - client_name: The PServer client name.
            - client_pass: The PServer client's password.
            - feature_names: A python list of feature names.
            - user_twitter_id: A Twitter user identifier.
            - topic_to_score: A python dictionary that maps from topic to score.
    """

    # Construct values.
    values = "usr=" + str(user_twitter_id)

    for topic, score in topic_to_score.items():
        values += "&type." + topic + "=%.2f" % score

    # Construct request.
    request = construct_request(model_type="pers",
                                client_name=client_name,
                                client_pass=client_pass,
                                command="setusr",
                                values=values)

    # Send request.
    send_request(host_name,
                 request)


def construct_request(model_type, client_name, client_pass, command, values):
    """
    Construct the request url.

    Inputs: - model_type: PServer usage mode type.
            - client_name: The PServer client name.
            - client_pass: The PServer client's password.
            - command: A PServer command.
            - values: PServer command arguments.

    Output: - base_request: The base request string.
    """
    base_request = ("{model_type}?"
                    "clnt={client_name}|{client_pass}&"
                    "com={command}&{values}".format(model_type=model_type,
                                                    client_name=client_name,
                                                    client_pass=client_pass,
                                                    command=command,
                                                    values=values))
    return base_request


def send_request(host_name, request):
    """
    Sends a PServer url request.

    Inputs: - host_name: A string containing the address of the machine where the PServer instance is hosted.
            - request: The url request.
    """
    request = "%s%s" % (host_name, request)
    print(request)
    try:
        result = requests.get(request)
        if result.status_code == 200:
            return result
        else:
            print(result.status_code)
            raise Exception
    except Exception as e:
        print(e)
        raise e


def update_feature_value(host_name, client_name, client_pass, feature_names):
    """
    TODO: This needs to be completed.
    """
    username = data[0]
    feature_value = '{0:.7f}'.format(data[1])
    joined_ftr_value = 'ftr_'+feature+'='+str(feature_value)
    values = 'usr=%s&%s' % (username, joined_ftr_value)

    # Construct request.
    request = construct_request(model_type="pers",
                                client_name=client_name,
                                client_pass=client_pass,
                                command="setusr",
                                values=values)

    # Send request.
    send_request(host_name,
                 request)
