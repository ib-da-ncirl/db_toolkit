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
from db_toolkit.CosmosDb import CosmosDb


class TestCosmosDb(TestCase):

    def test_load_cfgfile(self):
        # check less than min constructor arguments
        self.assertRaises(ValueError, CosmosDb)
        self.assertRaises(ValueError, CosmosDb, endpoint='endpoint')

        endpoint = 'my db endpoint'
        key = 'my db key'
        database_name = 'my db name'
        container_name = 'my container name'

        strIo = StringIO()
        strIo.write(' # this is a comment\n')
        strIo.write(f'endpoint: {endpoint}\n')
        strIo.write(f'key: {key}\n')
        strIo.write(f'database_name: {database_name}\n')
        strIo.write(f'container_name: {container_name}\n')
        strIo.seek(0, SEEK_SET)

        CosmosDb.__testmode__ = True
        db = CosmosDb(endpoint='fake endpoint', key='fake key')

        self.assertRaises(ValueError, db._load_cfgfile, None)

        db._load_cfgfile(strIo)
        self.assertEqual(db.endpoint, endpoint)
        self.assertEqual(db.key, key)
        self.assertEqual(db.database_name, database_name)
        self.assertEqual(db.container_name, container_name)

        strIo.truncate(0)
        strIo.seek(0, SEEK_SET)
        strIo.write(f'endpoint: \n')
        self.assertRaises(ValueError, db._load_cfgfile, strIo)

        strIo.truncate(0)
        strIo.seek(0, SEEK_SET)
        strIo.write(f': value\n')
        self.assertRaises(ValueError, db._load_cfgfile, strIo)

        strIo.close()

        self.assertRaises(ValueError, CosmosDb, cfgfilename='doesnotexist')
        self.assertRaises(ValueError, CosmosDb, cfgfilename=['i am a list'])

        CosmosDb.__testmode__ = False


if __name__ == '__main__':
    unittest.main()
