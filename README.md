reveal-user-annotation
======================

Utility methods for generating labels for Twitter users and handling their storage and retrieval.

Includes
--------
- Twitter user annotation based on crowdsourcing Twitter lists.
- Utility functions for integration with [MongoDB](http://www.mongodb.org/), [RabbitMQ](http://www.rabbitmq.com) and [PServer](http://www.pserver-project.org/)
- Basic text pre-processing pipeline utilities.

Install
-------
### Required packages
- numpy
- scipy
- nltk
- twython
- pymongo
- celery
- networkx

### Installation
To install for all users on Unix/Linux:

    python3.4 setup.py build
    sudo python3.4 setup.py install
  
Alternatively:

    pip install reveal-user-annotation

Notes
-----
### Stopwords
In the `.../reveal_user_annotation/text/res/stopwords/*` folder, there exist several files containing stopwords.

Among them, distributed are the [Google english stopwords](https://code.google.com/p/stop-words/) which are free to use.
