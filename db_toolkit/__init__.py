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

from .cosmosdb.CosmosDb import CosmosDb
from .cosmosdb.cosmosdb_sql import select
from .cosmosdb.cosmosdb_sql import property_quote_if

from .mongo.MongoDb import MongoDb

from .postgres.PostgresDb import PostgresDb
from .postgres.postgresdb_sql import does_table_exist_sql
from .postgres.postgresdb_sql import count_sql
from .postgres.postgresdb_sql import estimate_count_sql


# if somebody does "from db_toolkit import *", this is what they will
# be able to access:
__all__ = [
    'CosmosDb',
    'select',
    'property_quote_if',
    'MongoDb',
    'PostgresDb',
    'does_table_exist_sql',
    'count_sql',
    'estimate_count_sql',
]
