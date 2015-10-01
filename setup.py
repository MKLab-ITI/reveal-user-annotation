__author__ = 'Georgios Rizos (georgerizos@iti.gr)'

from setuptools import setup


def readme():
    with open("README.md") as f:
        return f.read()

setup(
    name='reveal-user-annotation',
    version='0.1.19',
    author='Georgios Rizos',
    author_email='georgerizos@iti.gr',
    packages=['reveal_user_annotation',
              'reveal_user_annotation.common',
              'reveal_user_annotation.text',
              'reveal_user_annotation.twitter',
              'reveal_user_annotation.mongo',
              'reveal_user_annotation.pserver',
              'reveal_user_annotation.rabbitmq',
              'reveal_user_annotation.entry_points'],
    url='https://github.com/MKLab-ITI/reveal-user-annotation',
    license='Apache',
    description='Utility methods for generating labels for Twitter users and handling their storage and retrieval',
    long_description=readme(),
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Science/Research',
        'License :: OSI Approved :: Apache Software License',
        'Natural Language :: English',
        'Operating System :: Unix',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3 :: Only',
        'Programming Language :: Python :: Implementation',
        'Programming Language :: Python :: Implementation :: CPython',
        'Topic :: Scientific/Engineering :: Information Analysis'
    ],
    keywords="online-social-network user-annotation twitter-list-crowdsourcing Reveal-FP7",
    entry_points={
        'console_scripts': ['store_snow_tweets_in_mongo=reveal_user_annotation.entry_points.store_snow_tweets_in_mongo:main',
                            'extract_twitter_list_keywords=reveal_user_annotation.entry_points.extract_twitter_list_keywords:main'],
    },
    package_data={'reveal_user_annotation.text': ['res/stopwords/*.txt'],
                  'reveal_user_annotation.twitter': ['res/topics/*.txt']},
    include_package_data=False,
    install_requires=open("requirements.txt").read().split("\n")
)
