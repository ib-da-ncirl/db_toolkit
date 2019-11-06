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

ALIAS = 'f'


def property_quote_if(alias, name):
    """
    Make a property reference, escaping it if necessary
    :param alias: object alias
    :param name: property name
    :return: property reference
    """
    if ' ' in name:
        # This syntax is useful to escape a property that contains spaces, special characters, or has the same name as a
        # SQL keyword or reserved word.
        prop = f'["{name}"]'
    else:
        prop = f'.{name}'
    return f'{alias}{prop}'


def select(container_name, selection, alias=ALIAS, project=None, where=None):
    """
    Generate a cosmosDb SQL select statement
    :param container_name: Name of container
    :param selection: entries to select; may be
            - string, e.g. '*'
            - list, e.g. ['id', 'name']
            - dict, e.g. {
    :param alias:
    :param project:
    :param where: dict with 'key' as the property and 'value' as the required value for the
    :return:
    """
    sql = 'SELECT '
    if isinstance(selection, dict):
        # SELECT {"Name":f.id, "City":f.address.city} AS Family
        #     FROM Families f
        #     WHERE f.address.city = f.address.state
        if project is not None:
            sql += '{'

        count = len(selection) - 1
        for key in selection.keys():
            sql += f'"{key}":{property_quote_if(alias, selection[key])}'
            if count > 0:
                sql += ', '
            count -= 1

        if project is not None:
            sql += '} '

        if project is not None:
            sql += f'AS {project} '

    elif isinstance(selection, list):
        # SELECT f.id, f.address.city
        #     FROM Families f
        #     WHERE f.address.city = f.address.state
        count = len(selection) - 1
        for value in selection:
            sql += f'{property_quote_if(alias, value)}'
            if count > 0:
                sql += ', '
            count -= 1

        sql += ' '

    else:
        sql += f'{selection} '

    sql += f'FROM {container_name} {alias} '

    if where is not None:
        sql += f'WHERE '
        count = len(where) - 1
        for key in where.keys():
            sql += f'{property_quote_if(alias, key)} = {where[key]}'
            if count > 0:
                sql += ','
            count -= 1

    return sql.strip()




