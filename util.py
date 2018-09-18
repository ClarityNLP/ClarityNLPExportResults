"""
Utilities file for reading required properties
"""

import configparser
from os import getenv, environ, path

config = configparser.RawConfigParser()
config.read('./project.cfg')

def read_property(env_name, config_tuple):
    property_name = ''
    if getenv(env_name):
        property_name = environ.get(env_name)
    else:
        try:
            property_name = config.get(config_tuple[0], config_tuple[1])
        except Exception as ex:
            print(ex)
    return property_name


# Getting the OHDSI DB type
ohdsi_db_type = read_property('OHDSI_DATABASE_TYPE', ('ohdsi', 'dbtype'))

# Getting the PHDSI DB schema
ohdsi_schema = read_property('OHDSI_SCHEMA', ('ohdsi', 'schema'))

# Getting the OHDSI DB credentials
ohdsi_conn_string = "host='%s' dbname='%s' user='%s' password='%s' port=%s" % (read_property('OHDSI_HOSTNAME', ('ohdsi', 'host')),
                                                                         read_property('OHDSI_DATABASE', ('ohdsi', 'dbname')),
                                                                         read_property('OHDSI_USER', ('ohdsi', 'user')),
                                                                         read_property('OHDSI_PASSWORD', ('ohdsi', 'password')),
                                                                         str(read_property('OHDSI_CONTAINER_PORT', ('ohdsi', 'port'))))

# Getting the mongo DB credentials for ClarityNLP results
mongo_host = read_property('NLP_MONGO_HOSTNAME', ('mongo', 'host'))
mongo_port = read_property('NLP_MONGO_CONTAINER_PORT', ('mongo', 'port'))
mongo_db = read_property('NLP_MONGO_DATABASE', ('mongo', 'db'))
mongo_working_index = read_property('NLP_MONGO_WORKING_INDEX', ('mongo', 'working_index'))
mongo_working_collection = read_property('NLP_MONGO_WORKING_COLLECTION', ('mongo', 'working_collection'))
