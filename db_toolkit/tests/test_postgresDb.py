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

import unittest
from unittest import TestCase
from postgres.PostgresDb import PostgresDb
from io import StringIO
from io import SEEK_SET
from testfixtures import LogCapture
from testfixtures import StringComparison
import os

# indices of argument keys in PostgresDb.KEYS
user_idx = 0
password_idx = 1
dbname_idx = 2
host_idx = 3
port_idx = 4

# argument which may be used for testing config file syntax, but are not real
# need to be same order as PostgresDb.KEYS
values = (
    'username',
    'verysecret',
    'my database',
    'http://myserver',
    '3000'
)


class TestPostgresDb(TestCase):
    """
    PostgresDb tests
    Note: a valid configuration file is required for some of the tests
          Specify the path to the file in the environment variable POSTGRES_CFG.
    """

    def test_load_cfg_file(self):

        # check less than min constructor arguments
        self.assertRaises(ValueError, PostgresDb)
        for usr in [None, values[user_idx]]:
            for passwd in [None, values[password_idx]]:
                for db in [None, values[dbname_idx]]:
                    if usr is None or passwd is None or db is None:
                        self.assertRaises(ValueError, PostgresDb, user=usr, password=passwd, dbname=db)

        # valid config file
        str_io = StringIO()
        str_io.write(' # this is a comment\n')
        for idx in list(range(len(PostgresDb.KEYS))):
            str_io.write(f'{PostgresDb.KEYS[idx]}= {values[idx]}\n')
        str_io.seek(0, SEEK_SET)

        db = PostgresDb(user='fake usr', password='fake passwd', dbname='fake db')

        # test missing config file handle
        self.assertRaises(ValueError, db._load_cfg_file, None)

        # test valid config file handle
        db._load_cfg_file(str_io)
        for idx in list(range(len(PostgresDb.KEYS))):
            self.assertEqual(db[PostgresDb.KEYS[idx]], values[idx])

        # test missing value
        str_io.truncate(0)
        str_io.seek(0, SEEK_SET)
        str_io.write(f'user= \n')
        self.assertRaises(ValueError, db._load_cfg_file, str_io)

        # test missing key
        str_io.truncate(0)
        str_io.seek(0, SEEK_SET)
        str_io.write(f'= value\n')
        self.assertRaises(ValueError, db._load_cfg_file, str_io)

        # invalid separator
        str_io.truncate(0)
        str_io.seek(0, SEEK_SET)
        str_io.write(f'user: value\n')
        self.assertRaises(ValueError, db._load_cfg_file, str_io)

        # unknown key/value
        str_io.truncate(0)
        str_io.seek(0, SEEK_SET)
        str_io.write(f'unknown_key= value\n')
        with LogCapture() as log_cap:
            db._load_cfg_file(str_io)
            log_cap.check(
                ('root', 'INFO', 'Ignoring unknown entry on line 1'),
            )

        str_io.close()

        # test invalid config file path
        self.assertRaises(ValueError, PostgresDb, cfg_filename='doesnotexist')
        # test invalid config file argument type
        self.assertRaises(ValueError, PostgresDb, cfg_filename=['i am a list'])

    def get_test_database(self):
        """
        Get the test database
        Note: a valid configuration file is required.
              Specify the path to the file in the environment variable POSTGRES_CFG.
        """
        cfg_filename = os.environ.get('POSTGRES_CFG')
        if cfg_filename is None:
            self.fail('Configuration file not set. '
                      'Please specify path to configuration file as environment variable POSTGRES_CFG')

        return PostgresDb(cfg_filename=cfg_filename)

    def test_connection(self):
        """
        Run a unit test on the connection method
        Note: a valid configuration file is required.
              Specify the path to the file in the environment variable POSTGRES_CFG.
        """
        db = self.get_test_database()
        valid_cfg = db.get_configuration()

        # test valid config
        self.assertFalse(db.is_connected())
        connection = db.get_connection()
        self.assertIsNotNone(connection)
        self.assertTrue(db.is_connected())
        db.close_connection()
        self.assertFalse(db.is_connected())

        # test erroneous connection config one argument at a time
        with LogCapture() as log_cap:
            for idx in list(range(len(PostgresDb.KEYS))):

                if idx == port_idx:
                    # TODO figure out way to test connection timeout, skip for now
                    continue

                db[PostgresDb.KEYS[idx]] = values[idx]
                connection = db.get_connection()
                self.assertIsNone(connection)

                db[PostgresDb.KEYS[idx]] = valid_cfg[PostgresDb.KEYS[idx]]

            auth_failed_regex = r'.*password authentication failed for user.*'
            log_cap.check(
                ('root', 'WARNING', StringComparison(auth_failed_regex)),
                ('root', 'WARNING', StringComparison(auth_failed_regex)),
                ('root', 'WARNING', StringComparison(r'.*does not exist.*')),
                ('root', 'WARNING', StringComparison(r'could not translate host name.*'))
            )

    def test_cursor(self):
        """
        Run a unit test on the cusor method
        Note: a valid configuration file is required.
              Specify the path to the file in the environment variable POSTGRES_CFG.
        """
        db = self.get_test_database()

        # test na when not connected
        self.assertFalse(db.is_connected())
        with LogCapture() as log_cap:
            cursor = db.cursor()
            self.assertIsNone(cursor)
        log_cap.check(
            ('root', 'WARNING', 'No connection available')
        )

        # test available when connected
        connection = db.get_connection()
        self.assertIsNotNone(connection)
        self.assertTrue(db.is_connected())
        cursor = db.cursor()
        self.assertIsNotNone(cursor)

        # PostgreSQL version
        cursor.execute("SELECT version();")
        record = cursor.fetchone()
        self.assertIsNotNone(record)
        cursor.close()

        db.close_connection()


if __name__ == '__main__':
    unittest.main()
