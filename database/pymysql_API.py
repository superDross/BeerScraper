''' Classes to ease the pain of interacting with mySQL DataBases and Tables.'''
# https://www.tutorialspoint.com/python3/python_database_access.htm
from tabulate import tabulate
import logging
import pandas as pd
import collections
import custom_exceptions
import sys


class DataBase(object):
    ''' Represents a mySQL database.

    Parameters:
        db: a pymysql.connect() object

    Example:
        db = pymysql.connect(host='localhost', user='root',
                             password='pass', db='beers')
        beers = DataBase(db)
        sql = "INSERT INTO BEERS (NAME) VALUES ('Test Beer')"
        beers.cmd(sql)
    '''

    def __init__(self, db):
        self.db = db

    def cmd(self, command):
        ''' Parse a command to MySQL and commit it.'''
        with self.db.cursor() as cursor:
            try:
                cursor.execute(command)
                self.db.commit()
            except Exception as e:
                logging.exception('message')
                self.db.rollback()
                self.close()
                sys.exit()


class Dict2Command(object):
    ''' Converts dictionary entries into mySQL commands and returns as a string.

    Attributes:
        self.dictionary = dict where keys are column in table, items are values.
                          used as to change column values in a SQL table.
        self.table = SQL table to apply the command to
        self.command = command type e.g. insert, update etc.
        self.where = a dict for WHERE command (key is column)

    Example:
        d = {'abv': 5, 'seasonal': 'N', 'srm': None}
        where = {'full_beer_name': 'Brewdog Punk IPA',
                 'brewery': 'Brewdog'}
        cmd = Dict2Command(dictionary=d, table='CRAFT_BEERS',
                           command='where', conditions=where)
    Returns:
        UPDATE CRAFT_BEERS SET abv = 5, seasonal = "N", srm = NULL
        WHERE full_beer_name = "Brewdog Punk IPA", brewery = "Brewdog"
        '''

    def __init__(self, dictionary, table, command='insert', conditions=None):
        self.dictionary = self._correct_items(dictionary)
        self.table = table
        self.command = command
        self.conditions = self._correct_items(conditions) if conditions else None
        self.funcs = {'insert': self._insert_cmd,
                      'update': self._update_cmd}

    def _key_equals_item_str(self, d):
        equals = ['{} = {}'.format(k, i)
                  for k, i in zip(d.keys(), d.values())]
        return ', '.join(equals)

    def _correct_items(self, dictionary):
        dictionary = collections.OrderedDict(dictionary)
        for k, i in dictionary.items():
            if isinstance(i, int) or isinstance(i, float):
                i = str(i)
            elif not i:
                i = 'NULL'
            elif isinstance(i, str):
                i = '"{}"'.format(i.replace("'", "").replace('"', ''))
            dictionary[k] = i
        return dictionary

    def _update_cmd(self):
        if not self.conditions:
            raise TypeError('conditions argument required to'
                            'construct an UPDATE command.')
        set_str = self._key_equals_item_str(self.dictionary)
        where_str = self._key_equals_item_str(self.conditions)
        update_cmd = 'UPDATE {} '.format(self.table)
        set_cmd = 'SET {}'.format(set_str)
        where_cmd = 'WHERE {}'.format(where_str)
        command = ' '.join([update_cmd, set_cmd, where_cmd])
        return command

    def _insert_cmd(self):
        keys = ', '.join(self.dictionary.keys())
        items = ', '.join(self.dictionary.values())
        sql_cmd = 'INSERT INTO {} ({}) VALUES ({})'.format(
            self.table, keys, items)
        return sql_cmd

    def _invalid_command(self):
        ''' Raise InvalidChoice exception if command not in funcs dict.'''
        msg = '{} is not a valid command argument. Only {} are valid.'
        valid = ', '.join(self.funcs.keys())
        msg = msg.format(self.command, valid)
        raise custom_exceptions.InvalidChoice(
            self.command, valid, msg)

    def __str__(self):
        ''' Returns SQL command after object intiation.'''
        func = self.funcs.get(self.command.lower())
        if not func:
            self._invalid_command()
        sql_cmd = func()
        return sql_cmd


class SQLTable(DataBase):
    ''' Represents a mySQL table.

    Example:
        db = pymysql.connect(host='localhost', user='root',
                             password='pass', db='beers')
        beers = SQLTable(db, 'Test Beer')
        beers.exists('beer_name', 'Punk IPA')
    '''

    def __init__(self, db, table):
        DataBase.__init__(self, db)
        self.table = table
        # keys, items filled during dict2cmd
        self.keys = None
        self.items = None
        self._valid_table()

    def _valid_table(self):
        ''' Raise an exception if table doesn't exist.'''
        command = "SELECT * FROM {}".format(self.table)
        with self.db.cursor() as cursor:
            cursor.execute(command)

    def dict2cmd(self, dictionary, command='insert', conditions=None):
        ''' Construct an SQL command from a dict, where
            the keys are column names in table and items are values.
        '''
        sql_cmd = Dict2Command(dictionary=dictionary, table=self.table,
                               command=command, conditions=conditions)
        return str(sql_cmd)

    def print(self, command):
        ''' Print the command.'''
        with self.db.cursor() as cursor:
            cursor.execute(command)
            tupled_table = cursor.fetchall()
            cursor.execute('DESCRIBE {}'.format(self.table))
            column_names = [x[0] for x in cursor.fetchall()]
            table = tabulate(tupled_table, column_names)
            print(table)

    def column2list(self, column):
        ''' Return all values in a given column as a list.'''
        with self.db.cursor() as cursor:
            cursor.execute('SELECT {} FROM {}'.format(column, self.table))
            column_values = [x[0] for x in cursor.fetchall()]
            return column_values

    def exists(self, column, query):
        ''' Checks if a query is present within a given table
            column. Returns a Boolean.
        '''
        num2bool = {1: True, 0: False}
        base_command = "SELECT * FROM {} WHERE {} = '{}'"
        command = base_command.format(self.table, column, query)
        with self.db.cursor() as cursor:
            exist = cursor.execute(command)
            return num2bool.get(exist)

    def to_pandas(self):
        ''' Convert a database table to pandas DataFrame.'''
        command = 'SELECT * FROM {}'.format(self.table)
        df = pd.read_sql(command, con=self.db)
        return df

    def close(self):
        ''' Close the database object.'''
        self.db.close()
