"""
    Flask Configuration management object
"""

import os

app_config = {
    "host": 'localhost', 
    "port": 3306,
    "user": 'root', 
    "password": 'sarada', 
    "db": 'epiviz',
    "unix_socket": "/run/mysqld/mysqld.sock"
}

# class Config(object):
#     """
#         Config sets various flask parameters
#     """
#     DEBUG = True
#     TESTING = False
#     CSRF_ENABLED = True
#     THREADED = True
#     SECRET = "BJbfSFrdtfNIxE5F0j5a6tGcV1ghAH"
#     SQLALCHEMY_DATABASE_URI = "mysql+pymysql://root:<KEY>@localhost/epiviz"
#     SQLALCHEMY_TRACK_MODIFICATIONS = False

# app_config = Config
