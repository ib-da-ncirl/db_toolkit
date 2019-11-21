# The MIT License (MIT)
# Copyright (c) 2019 Ian Buttimer

# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:

# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.

# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

import logging
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, OperationFailure, BulkWriteError

import urllib.parse
from time import sleep
from datetime import timedelta

from pymongo.results import InsertManyResult

from db_toolkit.misc.config_reader import (
    load_cfg_file,
    load_cfg_filename
)

logging.basicConfig(level=logging.DEBUG)


class MongoDb:
    """
    MongoDb client
    """

    REQUIRED_KEYS = (
        'server',  # The server ip address/url, e.g. 'mymongodb.server.com'
    )
    BASE_KEYS = REQUIRED_KEYS + (
        'username',  # username to login in with
        'password',  # password to login in with
        'port',  # port number to use, default to 27017
        'dbname',  # name of the database
        'collection'  # name of the collection to work with
    )
    QUERY_KEYS = (
        'auth_source',  # name of the authentication database
        'ssl',  # boolean to enable or disables TLS/SSL for the connection
        'replica_set',  # the name of the replica set
        'max_idle_time_ms',  # maximum number of milliseconds that a connection can remain idle
        'app_name',  # custom app name
        'retry_writes'  # enable retryable writes
    )
    KEYS = BASE_KEYS + QUERY_KEYS
    LINK_ORDER = (  # order parameters will appear in a connection string
                     'username',
                     'password',
                     'server',
                     'port',
                     'dbname',
                 ) + QUERY_KEYS

    QUERY_KEY_NAMES = (  # names to pass as keys in query string, (*follows same order as QUERY_KEYS!*)
        'authSource',
        'ssl',
        'replicaSet',
        'maxIdleTimeMS',
        'appName',
        'retryWrites'
    )

    def __init__(self, **kwargs):
        """
        Initialise object
        :param cfg_filename: Path of configuration file
        :param server: The server ip address/url, e.g. 'mymongodb.server.com'
        :param username: username to login in with
        :param password: password to login in with
        :param port: port number to use, default to 27017
        :param dbname: name of the database
        :param auth_source: name of the authentication database
        :param collection: name of the collection in the database
        :param ssl: boolean to enable or disables TLS/SSL for the connection
        :param replica_set: the name of the replica set
        :param max_idle_time_ms: maximum number of milliseconds that a connection can remain idle
        :param app_name: custom app name
        :param test: test mode flag; default False
        """
        for key in MongoDb.KEYS:
            self[key] = None
        for key in kwargs:
            if key not in ['test', 'cfg_filename', 'cfg_bundle']:
                self[key] = kwargs[key]

        if 'cfg_filename' in kwargs:
            self._load_cfg_filename(kwargs['cfg_filename'])
        elif 'cfg_bundle' in kwargs:
            self.__set_config(kwargs['cfg_bundle'])

        # check for missing required keys
        for key in MongoDb.REQUIRED_KEYS:
            if self[key] is None:
                raise ValueError(f'Missing {key} configuration')

        # if not in test mode, create client
        create = True
        if 'test' in kwargs:
            create = not kwargs['test']
        if create:
            self.client = MongoClient(self.make_db_link(), retryWrites=False)

    @staticmethod
    def __test_params(args):
        """
        Verify the specified params are correct
        :param args: dict of params
        """
        for key in MongoDb.REQUIRED_KEYS:
            if args[key] is None:
                raise ValueError(f'Missing {key} configuration')

        if args['username'] is None and args['password'] is not None:
            raise ValueError('Password configured but no username configured')
        for key in ['port', 'max_idle_time_ms']:
            if args[key] is not None and not args[key].isdigit():
                raise ValueError(f'Non-integer value specified for {key}')
        for key in ['ssl', 'retry_writes']:
            if args[key] is not None:
                lwr = args[key].lower()
                if not (lwr == 'true' or lwr == 'false'):
                    raise ValueError(f'Invalid value specified for {key}; must be "true" or "false"')

    def __set_config(self, config):
        """
        Set the configuration
        :param config: dict with settings
        """
        for key in config.keys():
            if key in MongoDb.KEYS:
                self[key] = config[key]

    def _load_cfg_file(self, cfg_file):
        """
        Read settings from specified configuration file
        :param cfg_file: Configuration file descriptor to load
        """
        self.__set_config(load_cfg_file(cfg_file, MongoDb.KEYS))

    def _load_cfg_filename(self, cfg_filename):
        """
        Read settings from specified configuration file
        :param cfg_filename: Path of configuration file to load
        """
        self.__set_config(load_cfg_filename(cfg_filename, MongoDb.KEYS))

    def make_db_link(self, **kwargs):
        """
        Create a database link
        Accepts same key arguments as constructor
        :return: database link
        :rtype: string
        """
        if 'server' in kwargs:
            args = {'server': kwargs['server']}
        else:
            args = {'server': self['server']}
        if args['server'] is None:
            raise ValueError('Server not configured')

        for key in MongoDb.KEYS:
            if key != 'server':
                if not self.__valid_key_check(key, False):
                    logging.warning(f'Ignoring unknown key "{key}"')
                    continue
                if key in kwargs:
                    args[key] = kwargs[key]
                else:
                    args[key] = self[key]

        MongoDb.__test_params(args)

        if args['ssl'] is not None:
            args['ssl'] = args['ssl'].lower()

        query_params = 0
        link = 'mongodb://'
        for key in MongoDb.LINK_ORDER:
            if args[key] is not None:
                if key == 'username':
                    link += f'{urllib.parse.quote_plus(args[key])}'
                elif key == 'password':
                    link += f':{urllib.parse.quote_plus(args[key])}@'
                elif key == 'server':
                    link += f'{args[key]}'
                elif key == 'port':
                    link += f':{args[key]}'
                # elif key == 'dbname':
                #     link += f'/{args[key]}'
                elif key in MongoDb.QUERY_KEYS:
                    if query_params == 0:
                        link += f'/?'
                    else:
                        link += f'&'
                    query_params += 1
                    q_idx = MongoDb.QUERY_KEYS.index(key)
                    link += f'{MongoDb.QUERY_KEY_NAMES[q_idx]}={args[key]}'

        logging.debug(link)
        return link

    def get_connection(self):
        """
        Establish a connection to the database, or return the existing connection
        :return: database connection
        """
        if not self.is_connected():
            try:
                self.client = MongoClient(self.make_db_link())

                logging.info(self.client.server_info())
            except OperationFailure as of:
                logging.warning(f'get_connection: {of}')
                self.close_connection()
            except Exception as dbError:
                logging.warning(dbError)
                self.client = None

        return self.client

    def is_connected(self):
        """
        Check if connected to database
        :return: True if connected
        """
        connected = False
        try:
            # The ismaster command is cheap
            if self.client is not None:
                self.client.admin.command('ismaster')
                connected = True
        except OperationFailure as of:
            logging.warning(f'is_connected : {of}')
        except ConnectionFailure:
            logging.warning("Server not available")
        return connected

    def is_authenticated(self):
        """
        Check if connected to database and user is authenticated
        :return: True if authenticated
        """
        authenticated = self.is_connected()
        if authenticated:
            try:
                self.client.server_info()
            except OperationFailure as of:
                logging.warning(f'is_authenticated: {of}')
                authenticated = False
        return authenticated

    def close_connection(self):
        """
        Close connection
        """
        if self.client is not None:
            self.client.close()
            self.client = None

    def get_database(self):
        """
        Get the database for this object
        :return: Database or None
        :rtype: pymongo.database.Database
        """
        db = None
        connection = self.get_connection()
        if connection is not None:
            if self['dbname'] is not None:
                db = connection[self['dbname']]
            else:
                logging.warning(f'Database not specified: {self["server"]}')
        return db

    def get_collection(self):
        """
        Get the collection for this object
        :return: Collection or None
        :rtype: pymongo.collection.Collection
        """
        collection = None
        db = self.get_database()
        if db is not None:
            if self['collection'] is not None:
                collection = db[self['collection']]
            else:
                logging.warning(f'Collection not specified: {self["server"]}/{self["dbname"]}')
        return collection

    def insert_many(self, entries):
        """
        Insert an iterable of documents.
        In the event that a BulkWriteError occurs (using an azure server where the throughput (RU/s) is exceeded),
        this method will attempt to continue in a slower batched mode.
        :param entries: iterable of documents
        :return: pymongo.results.InsertManyResult
        """
        result = InsertManyResult((), False)
        collection = self.get_collection()
        if collection is not None:
            initial_num_docs = collection.count_documents({})
            try:
                result = collection.insert_many(entries)
            except BulkWriteError as bwe:
                logging.warning(f'BulkWriteError: {bwe.details}')

                write_err = bwe.details['writeErrors'][0]
                err_index = write_err['index']
                err_code = write_err['code']
                # there is a RetryAfterMs value in errmsg but skip it for now and use a big value
                # err_msg = write_err['errmsg']
                sleep(0.5)  # wait 500ms

                if err_code == 16500 and 'mongo.cosmos.azure' in self['server']:
                    # looks like an azure server is being used and the number of requests exceeded capacity
                    # lets try it in batches
                    batch_size = int(err_index * 0.25)
                    logging.info(f'Attempting to continue in batches of {batch_size}')

                    try:
                        inserted_ids = [None] * err_index   # can't get ObjectIds of uploaded before BulkWriteError
                        count = err_index
                        pause = 0.25     # wait 250ms between batches
                        for idx in range(err_index, len(entries), batch_size):
                            result = collection.insert_many(entries[idx:idx + batch_size])
                            inserted_ids.append(result.inserted_ids)
                            count += len(result.inserted_ids)
                            estimate = int(((len(entries)-idx)/batch_size) * pause)
                            logging.info(f'Uploaded {count} of {len(entries)}, ETC {str(timedelta(seconds=estimate))}')
                            sleep(pause)

                        num_docs = collection.count_documents({}) - initial_num_docs
                        if num_docs != len(entries):
                            raise ValueError(f'Batch inserted document count {num_docs} '
                                             f'does not match document size of document collection {len(entries)}')
                        else:
                            result = InsertManyResult(inserted_ids, result.acknowledged)
                    except BulkWriteError as bweb:
                        logging.warning(f'BulkWriteError: {bweb.details}')
                        raise
                else:
                    raise
        return result

    def get_configuration(self):
        """
        Return a dictionary with a copy of the configuration for this object
        :return: configuration
        :rtype: dict
        """
        # new dict excluding non-config properties of object
        dict_copy = {key: self.__dict__[key] for key in self.__dict__.keys() if key in MongoDb.KEYS}
        return dict_copy

    @staticmethod
    def __valid_key_check(key, fatal=True):
        """
        Check that the specified key is valid
        :param key: object property name
        :param fatal: Raise error if invalid flag
        :return: True if valid
        """
        valid = key in MongoDb.KEYS
        if not valid and fatal:
            raise ValueError(f'The key "{key}" is not valid')
        return valid

    def __setitem__(self, key, value):
        """
        Implement assignment to self[key]
        :param key: object property name
        :param value: value to assign
        """
        self.__valid_key_check(key)
        self.__dict__[key] = value

    def __getitem__(self, key):
        """
        Implement evaluation of self[key]
        :param key: object property name
        """
        self.__valid_key_check(key)
        return self.__dict__[key]
