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

from io import SEEK_SET
from os import path
import re
import logging

import azure.cosmos
from azure.cosmos.cosmos_client import CosmosClient
import azure.cosmos.documents as documents

from db_toolkit.cosmosdb_sql import ALIAS
from db_toolkit.cosmosdb_sql import select

logging.basicConfig(level=logging.DEBUG)


class CosmosDb:
    """
    CosmosDb client
    """
    __testmode__ = False

    client = None

    endpoint = None
    key = None
    database_name = None
    container_name = None
    partition_key = None

    def __init__(self, cfgfilename=None, endpoint=None, key=None, database_name=None, container_name=None):
        self.endpoint = endpoint
        self.key = key
        self.database_name = database_name
        self.container_name = container_name
        if cfgfilename is not None:
            self._load_cfgfilename(cfgfilename)

        if self.endpoint is None:
            raise ValueError('Missing endpoint configuration')
        if self.key is None:
            raise ValueError('Missing key configuration')

        if not CosmosDb.__testmode__:
            self.client = CosmosClient(self.endpoint, {'masterKey': self.key})

    def _load_cfgfile(self, cfgfile):
        """
        Read settings from specified configuration file
        :param cfgfile: Configuration file descriptor to load
        """
        if cfgfile is None:
            raise ValueError('Missing configuration file argument')

        cfgfile.seek(0, SEEK_SET)  # seek start of file
        count = 0
        for line in cfgfile:
            line = line.strip()
            count += 1

            # skip blank or commented lines
            if len(line) == 0:
                continue
            if line.startswith('#'):
                continue

            keyval = re.match(r'(\w+):(.*)', line)
            if not keyval:
                raise ValueError(f'Invalid configuration file entry: line {count}')

            key = keyval.groups()[0].lower().strip()
            if len(key) == 0:
                raise ValueError(f'Missing key entry: line {count} ')
            value = keyval.groups()[1].strip()
            if len(value) == 0:
                raise ValueError(f'Missing value entry: line {count} ')
            if key == 'endpoint':
                self.endpoint = value
            elif key == 'key':
                self.key = value
            elif key == 'database_name':
                self.database_name = value
            elif key == 'container_name':
                self.container_name = value

    def _load_cfgfilename(self, cfgfilename):
        """
        Read settings from specified configuration file
        :param cfgfilename: Path of configuration file to load
        """
        if cfgfilename is None:
            raise ValueError('Missing configuration file argument')
        if not isinstance(cfgfilename, str):
            raise ValueError('Invalid configuration file argument: expected string')
        if not path.exists(cfgfilename):
            raise ValueError(f'Configuration file does not exist: {cfgfilename}')

        with open(cfgfilename, 'r') as cfgfile:
            self._load_cfgfile(cfgfile)

    def make_db_link(self, name=None):
        """
        Create a database link
        :param name: Optional database name, default is instance name
        :return: database link
        :rtype: string
        """
        if name is None:
            name = self.database_name
        if name is None:
            raise ValueError('Database name not configured')
        link = f'dbs/{name}'
        logging.debug(link)
        return link

    def make_container_link(self, name=None, dbname=None):
        """
        Create the container link
        :param name: Optional container name, default is instance container
        :param dbname: Optional database name, default is instance name
        :return: container link
        :rtype: string
        """
        if name is None:
            name = self.container_name
        if name is None:
            raise ValueError('Container name not configured')
        link = f'{self.make_db_link(name=dbname)}/colls/{name}'
        logging.debug(link)
        return link

    def make_doc_link(self, doc_id, container_name=None, dbname=None):
        """
        Create the container link
        :param doc_id: id of document
        :param container_name: Optional container name, default is instance container
        :param dbname: Optional database name, default is instance name
        :return: container link
        """
        link = f'{self.make_container_link(name=container_name, dbname=dbname)}/docs/{doc_id}'
        logging.debug(link)
        return link

    def create_database(self):
        """
        Create the database, if it does not already exit
        :return:
        """
        try:
            database = self.client.CreateDatabase({'id': self.database_name})
        except azure.cosmos.errors.HTTPFailure:
            database = self.client.ReadDatabase(self.make_db_link())
        return database

    def database_exists(self):
        """
        Check if database exists
        :return: True if database exists
        :rtype: bool
        """
        try:
            database = self.client.ReadDatabase(self.make_db_link())
        except azure.cosmos.errors.HTTPFailure as e:
            if e.status_code == azure.cosmos.http_constants.StatusCodes.NOT_FOUND:
                database = None
            else:
                raise e
        return database is not None

    def create_container(self, partition_path='/id'):
        """
        Create the container, if it does not already exit
        :param partition_path: The document path(s) to use as the partition key
        :return:
        """
        if self.container_name is None:
            raise ValueError('Container name not configured')

        if isinstance(partition_path, str):
            partition_path_list = [partition_path]
        elif isinstance(partition_path, list):
            partition_path_list = partition_path
        else:
            raise ValueError(f'Invalid partition path configuration: expected str or list, got {type(partition_path)}')

        self.partition_key = {
            'paths': partition_path_list,
            'kind': documents.PartitionKind.Hash
        }

        container_definition = {'id': self.container_name,
                                'partitionKey': self.partition_key}
        try:
            container = self.client.CreateContainer(self.make_db_link(), container_definition, {'offerThroughput': 400})
        except azure.cosmos.errors.HTTPFailure as e:
            if e.status_code == azure.cosmos.http_constants.StatusCodes.CONFLICT:
                container = self.client.ReadContainer(self.make_container_link())
            else:
                raise e

        return container

    def container_exists(self):
        """
        Create the container
        :return: True if container exists
        :rtype: bool
        """
        try:
            container = self.client.ReadContainer(self.make_container_link())
        except azure.cosmos.errors.HTTPFailure as e:
            if e.status_code == azure.cosmos.http_constants.StatusCodes.NOT_FOUND:
                container = None
            else:
                raise e

        return container is not None

    def upsert_item(self, item):
        """
        Upsert a document in a collection
        :param item: document to upsert
        :return: The upserted Document.
        :rtype: dict
        """
        # https://docs.microsoft.com/en-ie/python/api/azure-cosmos/azure.cosmos.cosmos_client.cosmosclient?view=azure-python#upsertitem-database-or-container-link--document--options-none-
        return self.client.UpsertItem(self.make_container_link(), item)

    def query_items_sql(self, query, options=None, partition_key=None):
        """
        Perform a query
        See https://docs.microsoft.com/en-ie/azure/cosmos-db/sql-query-getting-started
        :param query: SQL query string
        :param options: The request options for the request (default value None)
        :param partition_key: Partition key for the query (default value None)
        :return: List of json objects
        :rtype: List
        """
        # https://docs.microsoft.com/en-ie/python/api/azure-cosmos/azure.cosmos.cosmos_client.cosmosclient?view=azure-python#queryitems-database-or-container-link--query--options-none--partition-key-none-
        if options is None:
            options = {'enableCrossPartitionQuery': True}
        return self.client.QueryItems(self.make_container_link(), query, options=options, partition_key=partition_key)

    def query_items(self, selection, alias=ALIAS, project=None, where=None, options=None, partition_key=None):
        return self.query_items_sql(select(self.container_name, selection, alias=alias, project=project, where=where),
                                    options=options, partition_key=partition_key)

    def delete_items(self, partition_key, where=None):
        """

        :param where:
        :param partition_key: value of partition path for container; e.g. partition path = '/day', partition path = 'monday'
        :return:
        """
        # The SQL API in Cosmos DB does not support the SQL DELETE statement.
        deleted_items = []
        link = ''
        try:
            for item in self.query_items('*', where=where, options={'enableCrossPartitionQuery': True}):
                link = self.make_doc_link(item['id'])
                # TODO DeleteItem is supposed to return the deleted doc but only seems to be returning None
                deleted = self.client.DeleteItem(link, {'partitionKey': partition_key})
                deleted_items.append(deleted)
        except azure.cosmos.errors.HTTPFailure as e:
            if e.status_code == azure.cosmos.http_constants.StatusCodes.NOT_FOUND:
                logging.warning(f'Delete NOT_FOUND: {link} on partition "{partition_key}"')
                logging.warning(f' Valid partition keys are: {self.partition_key["paths"]}')
                deleted_items = []
            else:
                raise e

        return deleted_items
