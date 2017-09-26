"""
    Flask Configuration management object
"""

import os

class Config(object):
    """
        Config sets various flask parameters
    """
    DEBUG = True
    TESTING = False
    CSRF_ENABLED = True
    THREADED = True
    SECRET = "BJbfSFrdtfNIxE5F0j5a6tGcV1ghAH"
    SQLALCHEMY_DATABASE_URI = "mysql+pymysql://root:<KEY>@localhost/epiviz"
    SQLALCHEMY_TRACK_MODIFICATIONS = False

app_config = Config
