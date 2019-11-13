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

from unittest import TestCase
from cosmosdb.cosmosdb_sql import select
from cosmosdb.cosmosdb_sql import property_quote_if


class TestSelect(TestCase):
    def test_select(self):
        """
        Tests for the select clause
        See https://docs.microsoft.com/en-us/azure/cosmos-db/sql-query-select
        """
        # SELECT *
        #     FROM Families f
        container_name = 'Families'
        selection = '*'
        alias = 'f'
        self.assertEqual(f'SELECT * FROM {container_name} {alias}',
                         select(container_name, selection, alias=alias))
        # SELECT *
        #     FROM Families f
        #     WHERE f.id = "AndersenFamily"
        where = {
            "id": "AndersenFamily"
        }
        key = list(where.keys())[0]
        self.assertEqual(f'SELECT * FROM {container_name} {alias} WHERE {alias}.{key} = {where[key]}',
                         select(container_name, selection, alias=alias, where=where))

        # SELECT f.id, f.address.city
        #     FROM Families f
        #     WHERE f.id = "AndersenFamily"
        selection = ['id', 'address.city']
        self.assertEqual(f'SELECT {alias}.{selection[0]}, {alias}.{selection[1]} FROM {container_name} {alias} WHERE {alias}.{key} = {where[key]}',
                         select(container_name, selection, alias=alias, where=where))

        # SELECT *
        #     FROM Families f
        #     WHERE f["id is"] = "AndersenFamily"
        selection = '*'
        where = {
            "id is": "AndersenFamily"
        }
        key = list(where.keys())[0]
        self.assertEqual(f'SELECT * FROM {container_name} {alias} WHERE {alias}["{key}"] = {where[key]}',
                         select(container_name, selection, alias=alias, where=where))

        # SELECT {"Name":f.id, "City":f.address.city} AS Family
        #     FROM Families f
        #     WHERE f.address.city = f.address.state
        selection = {'Name': 'id', 'City': 'address.city'}
        project = 'Family'
        where = {
            "address.city": property_quote_if(alias, "address.state")
        }

        expected = 'SELECT {'
        count = len(selection) - 1
        for key in selection.keys():
            expected += f'"{key}":{alias}.{selection[key]}'
            if count > 0:
                expected += ', '
            count -= 1

        key = list(where.keys())[0]
        expected += '}' + f' AS {project} FROM {container_name} {alias} WHERE {alias}.{key} = {where[key]}'

        self.assertEqual(expected, select(container_name, selection, alias=alias, project=project, where=where))





