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
from mongo.MongoDb import MongoDb
from io import (
    StringIO,
    SEEK_SET
)
from testfixtures import (
    LogCapture,
    StringComparison
)
import os

# indices of argument keys in MongoDb.KEYS
server_idx = 0
user_idx = 1
password_idx = 2
port_idx = 3
dbname_idx = 4
collection_idx = 5
auth_src_idx = 6
ssl_idx = 7
replica_set_idx = 8
max_idle_time_ms_idx = 9
app_name_idx = 10
retry_writes_idx = 11

# argument which may be used for testing config file syntax, but are not real
# need to be same order as MongoDb.KEYS
values = (
    'myserver',
    'username',
    'verysecret',
    '28000',
    'mydatabase',
    'mycollection',
    'myauthsrc',
    'true',
    'myreplicaset',
    '1000',
    'myappname',
    'false'
)


class TestMongoDb(TestCase):
    """
    MongoDb tests
    Note: a valid configuration file is required for some of the tests
          Specify the path to the file in the environment variable MONGO_CFG.
    """

    def test_load_cfg_file(self):

        # check less than min constructor arguments
        self.assertRaises(ValueError, MongoDb)
        for svr in [None, values[server_idx]]:
            for usr in [None, values[user_idx]]:
                for passwd in [None, values[password_idx]]:
                    for port in [None, values[port_idx]]:
                        for db in [None, values[dbname_idx]]:
                            for auth in [None, values[auth_src_idx]]:
                                for coll in [None, values[collection_idx]]:
                                    if svr is None:
                                        self.assertRaises(ValueError, MongoDb, server=svr, username=usr,
                                                          password=passwd, port=port, dbname=db, auth_source=auth,
                                                          collection=coll, test=True)
                                    else:
                                        obj = MongoDb(server=svr, username=usr, password=passwd, port=port, dbname=db,
                                                      auth_source=auth, collection=coll, test=True)
                                        self.assertIsNotNone(obj)

        # valid config file
        str_io = StringIO()
        str_io.write(' # this is a comment\n')
        for idx in list(range(len(MongoDb.KEYS))):
            str_io.write(f'{MongoDb.KEYS[idx]}= {values[idx]}\n')
        str_io.seek(0, SEEK_SET)

        db = MongoDb(server='fake server', test=True)

        # test missing config file handle
        self.assertRaises(ValueError, db._load_cfg_file, None)

        # test valid config file handle
        db._load_cfg_file(str_io)
        for idx in list(range(len(MongoDb.KEYS))):
            self.assertEqual(db[MongoDb.KEYS[idx]], values[idx])

        # valid invalid port & timeout
        for test_key in ['port', 'max_idle_time_ms']:
            str_io.truncate(0)
            str_io.seek(0, SEEK_SET)
            for idx in list(range(len(MongoDb.KEYS))):
                if MongoDb.KEYS[idx] == test_key:
                    str_io.write(f'{MongoDb.KEYS[idx]}= notaninteger\n')
                else:
                    str_io.write(f'{MongoDb.KEYS[idx]}= {values[idx]}\n')
            db._load_cfg_file(str_io)
            self.assertRaises(ValueError, db.make_db_link)

            str_io.truncate(0)
            str_io.seek(0, SEEK_SET)
            for idx in list(range(len(MongoDb.KEYS))):
                if MongoDb.KEYS[idx] == test_key:
                    str_io.write(f'{MongoDb.KEYS[idx]}= {values[idx]}\n')
                else:
                    str_io.write(f'{MongoDb.KEYS[idx]}= 10.2\n')
            db._load_cfg_file(str_io)
            self.assertRaises(ValueError, db.make_db_link)

        # valid invalid ssl
        for test_key in ['ssl', 'retry_writes']:
            str_io.truncate(0)
            str_io.seek(0, SEEK_SET)
            for idx in list(range(len(MongoDb.KEYS))):
                if MongoDb.KEYS[idx] == test_key:
                    str_io.write(f'{MongoDb.KEYS[idx]}= notaboolean\n')
                else:
                    str_io.write(f'{MongoDb.KEYS[idx]}= {values[idx]}\n')
            db._load_cfg_file(str_io)
            self.assertRaises(ValueError, db.make_db_link)

        # test missing value
        str_io.truncate(0)
        str_io.seek(0, SEEK_SET)
        str_io.write(f'username= \n')
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
        self.assertRaises(ValueError, MongoDb, cfg_filename='doesnotexist')
        # test invalid config file argument type
        self.assertRaises(ValueError, MongoDb, cfg_filename=['i am a list'])

    def get_test_database(self):
        """
        Get the test database
        Note: a valid configuration file is required.
              Specify the path to the file in the environment variable MONGO_CFG.
        """
        cfg_filename = os.environ.get('MONGO_CFG')
        if cfg_filename is None:
            self.fail('Configuration file not set. '
                      'Please specify path to configuration file as environment variable MONGO_CFG')

        return MongoDb(cfg_filename=cfg_filename)

    def test_make_db_link(self):
        """
        Run a unit test on the db connection string
        """
        # test server only
        server = values[server_idx]
        db = MongoDb(server=server, test=True)
        link = db.make_db_link()
        self.assertEqual(f'mongodb://{server}', link)

        # test username, password & server
        username = values[user_idx]
        password = values[password_idx]
        db.username = username
        db.password = password
        link = db.make_db_link()
        self.assertEqual(f'mongodb://{username}:{password}@{server}', link)

        # test username, password, server & port
        port = values[port_idx]
        db.port = port
        link = db.make_db_link()
        self.assertEqual(f'mongodb://{username}:{password}@{server}:{port}', link)

        # test username, password, server, port & database
        dbname = values[dbname_idx]
        db.dbname = dbname
        link = db.make_db_link()
        self.assertEqual(f'mongodb://{username}:{password}@{server}:{port}', link)

        # test query arguments
        expected = f'mongodb://{username}:{password}@{server}:{port}/?'
        for idx in range(auth_src_idx, app_name_idx + 1):
            db[MongoDb.KEYS[idx]] = values[idx]
            if idx > auth_src_idx:
                expected += f'&'
            q_idx = MongoDb.QUERY_KEYS.index(MongoDb.KEYS[idx])
            expected += f'{MongoDb.QUERY_KEY_NAMES[q_idx]}={values[idx]}'
            link = db.make_db_link()
            self.assertEqual(expected, link)

        # clear query args
        for idx in range(auth_src_idx, app_name_idx + 1):
            db[MongoDb.KEYS[idx]] = None

        # test percent encoded username
        db.username = 'fun@ny:user/name%'
        encoded_username = 'fun%40ny%3Auser%2Fname%25'
        link = db.make_db_link()
        self.assertEqual(f'mongodb://{encoded_username}:{password}@{server}:{port}', link)

        # test percent encoded password
        db.password = '@:/%'
        encoded_password = '%40%3A%2F%25'
        link = db.make_db_link()
        self.assertEqual(f'mongodb://{encoded_username}:{encoded_password}@{server}:{port}', link)

    def test_connection(self):
        """
        Run a unit test on the connection method
        Note: a valid configuration file is required.
              Specify the path to the file in the environment variable MONGO_CFG.
        """
        client = self.get_test_database()
        valid_cfg = client.get_configuration()

        # test valid config
        self.assertTrue(client.is_connected())
        connection = client.get_connection()
        self.assertIsNotNone(connection)
        self.assertTrue(client.is_connected())
        client.close_connection()
        self.assertFalse(client.is_connected())

        real_password = client.password
        client.password = 'incorrect'
        connection = client.get_connection()
        self.assertFalse(client.is_authenticated())
        client.close_connection()

        client.password = real_password
        connection = client.get_connection()
        self.assertIsNotNone(connection)
        self.assertTrue(client.is_connected())
        db = client.get_database()
        self.assertIsNotNone(db)
        collection = client.get_collection()
        self.assertIsNotNone(collection)
        result = collection.insert_one({'hi': 'this is a test'})
        self.assertIsNotNone(result)
        self.assertIsNotNone(result.inserted_id)
        client.close_connection()


if __name__ == '__main__':
    unittest.main()
