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

import azure.cosmos.documents as documents
import azure.cosmos.errors as errors
import azure.cosmos.http_constants as http_constants
from azure.cosmos.cosmos_client import CosmosClient

from .cosmosdb_sql import ALIAS
from .cosmosdb_sql import select
from db_toolkit.misc.config_reader import load_cfg_file
from db_toolkit.misc.config_reader import load_cfg_filename

logging.basicConfig(level=logging.DEBUG)


class CosmosDb:
    """
    CosmosDb client
    """

    REQUIRED_KEYS = (
        'endpoint',         # URI of the database account
        'key'               # primary key of the database account
    )
    KEYS = REQUIRED_KEYS + (
        'dbname',           # name of the database
        'container_name'    # name of the database container
    )

    def __init__(self, cfg_filename=None, endpoint=None, key=None, dbname=None, container_name=None, test=False):
        """
        Initialise object
        :param cfg_filename: Path of configuration file
        :param endpoint: URI of the database account
        :param key: primary key of the database account
        :param dbname: name of the database
        :param container_name: name of the database container
        :param test: test mode flag; default False
        """
        self.endpoint = endpoint
        self.key = key
        self.dbname = dbname
        self.container_name = container_name
        self.partition_key = None
        if cfg_filename is not None:
            self._load_cfg_filename(cfg_filename)

        # check for missing required keys
        for key in CosmosDb.REQUIRED_KEYS:
            if self[key] is None:
                raise ValueError(f'Missing {key} configuration')

        # if not in test mode, create client
        if not test:
            self.client = CosmosClient(self.endpoint, {'masterKey': self.key})

    def __set_config(self, config):
        """
        Set the configuration
        :param config: dict with settings
        """
        for key in config.keys():
            if key in CosmosDb.KEYS:
                self[key] = config[key]

    def _load_cfg_file(self, cfg_file):
        """
        Read settings from specified configuration file
        :param cfg_file: Configuration file descriptor to load
        """
        self.__set_config(load_cfg_file(cfg_file, CosmosDb.KEYS))

    def _load_cfg_filename(self, cfg_filename):
        """
        Read settings from specified configuration file
        :param cfg_filename: Path of configuration file to load
        """
        self.__set_config(load_cfg_filename(cfg_filename, CosmosDb.KEYS))

    def make_db_link(self, name=None):
        """
        Create a database link
        :param name: Optional database name, default is instance name
        :return: database link
        :rtype: string
        """
        if name is None:
            name = self.dbname
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
            database = self.client.CreateDatabase({'id': self.dbname})
        except errors.HTTPFailure:
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
        except errors.HTTPFailure as e:
            if e.status_code == http_constants.StatusCodes.NOT_FOUND:
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
        except errors.HTTPFailure as e:
            if e.status_code == http_constants.StatusCodes.CONFLICT:
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
        except errors.HTTPFailure as e:
            if e.status_code == http_constants.StatusCodes.NOT_FOUND:
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
        """
        Perform a query
        See https://docs.microsoft.com/en-ie/azure/cosmos-db/sql-query-getting-started
        :param selection: entries to select; may be
                - string, e.g. '*'
                - list, e.g. ['id', 'name']
                - dict, e.g. {
        :param alias:
        :param project:
        :param where: dict with 'key' as the property and 'value' as the required value for the
        :param options: The request options for the request (default value None)
        :param partition_key: Partition key for the query (default value None)
        :return: List of json objects
        :rtype: List
        """
        return self.query_items_sql(select(self.container_name, selection, alias=alias, project=project, where=where),
                                    options=options, partition_key=partition_key)

    def delete_items(self, partition_key, where=None):
        """
        Delete an item(s) from the database
        :param partition_key: value of partition path for container; e.g. partition path = '/day', partition path = 'monday'
        :param where:
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
        except errors.HTTPFailure as e:
            if e.status_code == http_constants.StatusCodes.NOT_FOUND:
                logging.warning(f'Delete NOT_FOUND: {link} on partition "{partition_key}"')
                logging.warning(f' Valid partition keys are: {self.partition_key["paths"]}')
                deleted_items = []
            else:
                raise e

        return deleted_items

    def __setitem__(self, key, value):
        """
        Implement assignment to self[key]
        :param key: object property name
        :param value: value to assign
        """
        if key not in CosmosDb.KEYS:
            raise ValueError(f'The key "{key}" is not valid')
        self.__dict__[key] = value

    def __getitem__(self, key):
        """
        Implement evaluation of self[key]
        :param key: object property name
        """
        if key not in CosmosDb.KEYS:
            raise ValueError(f'The key "{key}" is not valid')
        return self.__dict__[key]

