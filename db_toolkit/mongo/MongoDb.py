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
from pymongo.errors import ConnectionFailure, OperationFailure

import urllib.parse

from db_toolkit.misc.config_reader import load_cfg_file
from db_toolkit.misc.config_reader import load_cfg_filename

logging.basicConfig(level=logging.DEBUG)


class MongoDb:
    """
    MongoDb client
    """

    REQUIRED_KEYS = (
        'server',  # The server ip address/url, e.g. 'mymongodb.server.com'
    )
    KEYS = REQUIRED_KEYS + (
        'username',     # username to login in with
        'password',     # password to login in with
        'port',         # port number to use, default to 27017
        'dbname',       # name of the database
        'auth_source',  # name of the authentication database
        'collection'    # name of the collection to work with
    )

    def __init__(self, cfg_filename=None, cfg_bundle=None, server=None, username=None, password=None, port=None,
                 dbname=None, auth_source=None, collection=None, test=False):
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
        :param test: test mode flag; default False
        """
        self.server = server
        self.username = username
        self.password = password
        self.port = port
        self.dbname = dbname
        self.auth_source = auth_source
        self.collection = collection
        if cfg_filename is not None:
            self._load_cfg_filename(cfg_filename)
        elif cfg_bundle is not None:
            self.__set_config(cfg_bundle)

        # check for missing required keys
        for key in MongoDb.REQUIRED_KEYS:
            if self[key] is None:
                raise ValueError(f'Missing {key} configuration')

        # if not in test mode, create client
        if not test:
            self.client = MongoClient(self.make_db_link())

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

    def make_db_link(self, server=None, username=None, password=None, port=None, dbname=None, auth_source=None):
        """
        Create a database link
        :param server: The server ip address/url, e.g. 'mymongodb.server.com'
        :param username: username to login in with
        :param password: password to login in with
        :param port: port number to use, default to 27017
        :param dbname: name of the database
        :param auth_source: name of the authentication database
        :return: database link
        :rtype: string
        """
        if server is None:
            server = self.server
        if server is None:
            raise ValueError('Server not configured')
        args = {'server': server}
        if username is None:
            username = self.username
        args['username'] = username
        if password is None:
            password = self.password
            if username is None and password is not None:
                raise ValueError('Password configured but no username configured')
        args['password'] = password
        if port is None:
            port = self.port
        args['port'] = port
        if dbname is None:
            dbname = self.dbname
        args['dbname'] = dbname
        if auth_source is None:
            auth_source = self.auth_source
        args['auth_source'] = auth_source

        link = 'mongodb://'
        for key in ('username', 'password', 'server', 'port', 'dbname', 'auth_source'):
            if args[key] is not None:
                if key == 'username':
                    link += f'{urllib.parse.quote_plus(args[key])}'
                elif key == 'password':
                    link += f':{urllib.parse.quote_plus(args[key])}@'
                elif key == 'server':
                    link += f'{args[key]}'
                elif key == 'port':
                    link += f':{args[key]}'
                elif key == 'dbname':
                    link += f'/{args[key]}'
                elif key == 'auth_source':
                    link += f'?authSource={args[key]}'

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
            if self.dbname is not None:
                db = connection[self.dbname]
            else:
                logging.warning(f'Database not specified: {self.server}')
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
            if self.collection is not None:
                collection = db[self.collection]
            else:
                logging.warning(f'Collection not specified: {self.server}/{self.dbname}')
        return collection

    def get_configuration(self):
        """
        Return a dictionary with a copy of the configuration for this object
        :return: configuration
        :rtype: dict
        """
        # new dict excluding non-config properties of object
        dict_copy = {key: self.__dict__[key] for key in self.__dict__.keys() if key in MongoDb.KEYS}
        return dict_copy

    def __setitem__(self, key, value):
        """
        Implement assignment to self[key]
        :param key: object property name
        :param value: value to assign
        """
        if key not in MongoDb.KEYS:
            raise ValueError(f'The key "{key}" is not valid')
        self.__dict__[key] = value

    def __getitem__(self, key):
        """
        Implement evaluation of self[key]
        :param key: object property name
        """
        if key not in MongoDb.KEYS:
            raise ValueError(f'The key "{key}" is not valid')
        return self.__dict__[key]

