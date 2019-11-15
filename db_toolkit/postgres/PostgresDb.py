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
import psycopg2

from db_toolkit.misc.config_reader import load_cfg_file
from db_toolkit.misc.config_reader import load_cfg_filename


# http://initd.org/psycopg/docs/connection.html


class PostgresDb:
    """
    PostgreSQL client
    """

    REQUIRED_KEYS = (
        'user',  # user name used to authenticate
        'password',  # password used to authenticate
        'dbname',  # the database name
    )
    KEYS = REQUIRED_KEYS + (
        'host',  # database host address (defaults to UNIX socket if not provided)
        'port'  # connection port number (defaults to 5432 if not provided)
    )

    def __init__(self, cfg_filename=None, user=None, password=None, dbname=None, host=None, port=None):
        """
        Initialise object
        :param cfg_filename: Path of configuration file
        :param user: user name used to authenticate
        :param password: password used to authenticate
        :param dbname: the database name
        :param host: database host address (defaults to UNIX socket if not provided)
        :param port: connection port number (defaults to 5432 if not provided)
        """
        self.user = user
        self.password = password
        self.dbname = dbname
        self.host = host
        self.port = port
        self.connection = None
        if cfg_filename is not None:
            self._load_cfg_filename(cfg_filename)

        for key in PostgresDb.REQUIRED_KEYS:
            if self[key] is None:
                raise ValueError(f'Missing {key} configuration')

    def __set_config(self, config):
        """
        Set the configuration
        :param config: dict with settings
        """
        for key in config.keys():
            if key in PostgresDb.KEYS:
                self[key] = config[key]

    def _load_cfg_file(self, cfg_file):
        """
        Read settings from specified configuration file
        :param cfg_file: Configuration file descriptor to load
        """
        self.__set_config(load_cfg_file(cfg_file, PostgresDb.KEYS))

    def _load_cfg_filename(self, cfg_filename):
        """
        Read settings from specified configuration file
        :param cfg_filename: Path of configuration file to load
        """
        self.__set_config(load_cfg_filename(cfg_filename, PostgresDb.KEYS))

    def get_connection(self):
        """
        Establish a connection to the database, or return the existing connection
        :return: database connection
        """
        if not self.is_connected():
            try:
                self.connection = psycopg2.connect(
                    user=self.user,
                    password=self.password,
                    host=self.host,
                    port=self.port,
                    dbname=self.dbname)

                # log PostgreSQL Connection properties
                logging.info(self.connection.get_dsn_parameters())

            except (Exception, psycopg2.Error) as dbError:
                logging.warning(dbError)
                self.connection = None

        return self.connection

    def is_connected(self):
        """
        Check if connected to database
        :return: True if connected
        """
        return self.connection is not None

    def close_connection(self):
        """
        Close connection
        """
        if self.is_connected():
            self.connection.close()
            self.connection = None

    def cursor(self):
        """
        Retrieve a cursor
        :return: Cursor or None if no connection available
        """
        if self.is_connected():
            # http://initd.org/psycopg/docs/cursor.html#cursor
            cursor = self.connection.cursor()
        else:
            cursor = None
            logging.warning('No connection available, cursor unavailable')
        return cursor

    def commit(self):
        """
        Commit any pending transaction to the database
        :return: Cursor or None if no connection available
        """
        if self.is_connected():
            # http://initd.org/psycopg/docs/connection.html
            self.connection.commit()
        else:
            logging.warning('No connection available, cannot commit')

    def get_configuration(self):
        """
        Return a dictionary with a copy of the configuration for this object
        :return: configuration
        :rtype: dict
        """
        # new dict excluding non-config properties of object
        dict_copy = {key: self.__dict__[key] for key in self.__dict__.keys() if key in PostgresDb.KEYS}
        return dict_copy

    def __setitem__(self, key, value):
        """
        Implement assignment to self[key]
        :param key: object property name
        :param value: value to assign
        """
        if key not in PostgresDb.KEYS:
            raise ValueError(f'The key "{key}" is not valid')
        self.__dict__[key] = value

    def __getitem__(self, key):
        """
        Implement evaluation of self[key]
        :param key: object property name
        """
        if key not in PostgresDb.KEYS:
            raise ValueError(f'The key "{key}" is not valid')
        return self.__dict__[key]
