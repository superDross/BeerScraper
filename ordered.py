import collections


def dict2sql(dictionary, table):
    ''' Construct a INSERT SQL command from a dict. Where the
        keys are column names in table and items are values.
    '''
    dictionary = collections.OrderedDict(dictionary)
    keys = ', '.join([k for k, i in dictionary.items()])
    items = []
    for k, i in dictionary.items():
        if isinstance(i, int) or isinstance(i, float):
            i = str(i)
        else:
            i = "'{}'".format(i)
        items.append(i)
    items = ', '.join(items)
    sql = 'INSERT INTO {} ({}) VALUES ({})'.format(table, keys, items)
    return sql
