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


def does_table_exist_sql(name):
    """
    Generate SQL to check if a table exists
    See https://www.dbrnd.com/2017/07/postgresql-different-options-to-check-if-table-exists-in-database-to_regclass/
    :param name: table name
    :return: SQL string
    """
    return f"SELECT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLES.TABLE_NAME='{name}');"


def count_sql(name):
    """
    Generate SQL to count the number of rows in a table
    :param name: table name
    :return: SQL string
    """
    return f'SELECT COUNT(*) FROM "{name}";'


def estimate_count_sql(name):
    """
    Generate SQL to estimate the number of rows in a table
    See https://wiki.postgresql.org/wiki/Count_estimate
    :param name: table name
    :return: SQL string
    """
    return f"SELECT reltuples::BIGINT AS estimate FROM pg_class WHERE relname='{name}';"


def drop_table_sql(name):
    """
    Generate SQL to drop a table
    :param name: table name
    :return: SQL string
    """
    return f"DROP TABLE IF EXISTS {name} CASCADE;"
