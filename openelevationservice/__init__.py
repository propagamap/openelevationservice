# -*- coding: utf-8 -*-

from os import getcwd, path, environ, makedirs
from yaml import safe_load

basedir = path.abspath(path.dirname(__file__))
SETTINGS = safe_load(open(path.join(basedir, 'server', 'ops_settings.yml')))

# Override settings with environment variable
for parameter in ('table_name', 'db_name', 'user_name', 'password', 'host', 'port'):
    env_name = "OES_" + parameter.upper()
    if env_name in environ:
        SETTINGS['provider_parameters'][parameter] = environ[env_name]


TILES_DIR = path.join(getcwd(), 'tiles')

if "TESTING" in environ:
    SETTINGS['provider_parameters']['table_name'] = SETTINGS['provider_parameters']['table_name'] + '_test'
    TILES_DIR = path.join(basedir, 'tests', 'tile')
    # if "CI" in environ:
    #     SETTINGS['provider_parameters']['port'] = 5433

if not path.exists(TILES_DIR):
    makedirs(TILES_DIR)
        

__version__ = "0.3.0"
