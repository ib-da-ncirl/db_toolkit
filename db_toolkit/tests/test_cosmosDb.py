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
from io import StringIO
from io import SEEK_SET
from cosmosdb.CosmosDb import CosmosDb
from testfixtures import LogCapture


class TestCosmosDb(TestCase):

    def test_load_cfg_file(self):

        # need to be same order as PostgresDb.KEYS
        values = (
            'my db endpoint',
            'my db key',
            'my db name',
            'my container name'
        )
        end_idx = 0
        key_idx = 1
        dbname_idx = 2
        contain_idx = 3

        # check less than min constructor arguments
        for end in [None, values[end_idx]]:
            for quay in [None, values[key_idx]]:
                if end is None or quay is None:
                    self.assertRaises(ValueError, CosmosDb, endpoint=end, key=quay)

        # valid config file
        str_io = StringIO()
        str_io.write(' # this is a comment\n')
        for idx in list(range(len(CosmosDb.KEYS))):
            str_io.write(f'{CosmosDb.KEYS[idx]}= {values[idx]}\n')
        str_io.seek(0, SEEK_SET)

        # test object
        db = CosmosDb(endpoint='fake endpoint', key='fake key', test=True)

        # test missing config file handle
        self.assertRaises(ValueError, db._load_cfg_file, None)

        # test valid config file handle
        db._load_cfg_file(str_io)
        for idx in list(range(len(CosmosDb.KEYS))):
            self.assertEqual(db[CosmosDb.KEYS[idx]], values[idx])

        # test missing value
        str_io.truncate(0)
        str_io.seek(0, SEEK_SET)
        str_io.write(f'endpoint= \n')
        self.assertRaises(ValueError, db._load_cfg_file, str_io)

        # test missing key
        str_io.truncate(0)
        str_io.seek(0, SEEK_SET)
        str_io.write(f'= value\n')
        self.assertRaises(ValueError, db._load_cfg_file, str_io)

        # invalid separator
        str_io.truncate(0)
        str_io.seek(0, SEEK_SET)
        str_io.write(f'endpoint: value\n')
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
        self.assertRaises(ValueError, CosmosDb, cfg_filename='doesnotexist')
        # test invalid config file argument type
        self.assertRaises(ValueError, CosmosDb, cfg_filename=['i am a list'])


if __name__ == '__main__':
    unittest.main()
