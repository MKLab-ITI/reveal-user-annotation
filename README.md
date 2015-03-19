reveal-user-annotation
======================

Utility methods for generating labels for Twitter users and handling their storage and retrieval.

Features
--------
- Twitter user annotation based on crowdsourcing Twitter lists.
- Basic text pre-processing pipeline utilities.
- Utility functions for integration with [MongoDB](http://www.mongodb.org/), [RabbitMQ](http://www.rabbitmq.com) and [PServer](http://www.pserver-project.org/)

Install
-------

To install for all users on Unix/Linux:

    python setup.py build
    sudo python setup.py install
  
Alternatively:

    pip install reveal-user-annotation

Reveal-FP7 Integration
----------------------
(Section under construction.)

Configuration
-----------
The package is configured as soon as it is imported.

The local resources and MongoDB configuration is mandatory for the usage of the reveal-user-annotation and [reveal-user-classification](https://github.com/MKLab-ITI/reveal-user-classification) packages.

### Edit config.txt
After the installation as described before, one must edit the `.../reveal_user_annotation/config.txt` file:

#### Local Resources
The relevant field is `DATA_PATH`.

`DATA_PATH`:                The local folder where the package data are stored.

#### MongoDB
The relevant fields are `mongod_path` and `dbpath`.

`mongod_path`:              The Mongo daemon executable in your local system.

`dbpath`:                   The data folder for the local MongoDB (as it should be in the mongodb.config file).

The above configurations are relevant to a local installation of MongoDB. Such an installation is used to store Twitter data that has been crawled using the [Twitter API](https://dev.twitter.com/rest/public) or intermediate document-type data.

#### PServer
The relevant fields are `PSERVER_HOST`, `ADMIN_PANEL_USER_NAME`, `ADMIN_PANEL_PASS`, `CLIENT_NAME` and `CLIENT_PASS`.

`PSERVER_HOST`:             The http address where the project PServer instance is located.

`ADMIN_PANEL_USER_NAME`:    PServer administration panel user name.

`ADMIN_PANEL_PASS`:         PServer administration panel password.

`CLIENT_NAME`:              The name of the client to store the data.

`CLIENT_PASS`:              The client password.

#### Twitter
The relevant fields are `APP_KEY`, `APP_SECRET` and `SCREEN_NAME`.

`APP_KEY`:                  Twitter app key.

`APP_SECRET`:               Twitter app secret.

`SCREEN_NAME`:              The twitter developer account screen name.

One must obviously have created a [Twitter app](https://apps.twitter.com/) and have a Twitter developer account.

Notes
-----

### Stopwords
In the `.../reveal_user_annotation/text/res/stopwords/*` folder, there exist several files containing stopwords.

Among them, distributed are the [Google english stopwords](https://code.google.com/p/stop-words/) which are free to use.
