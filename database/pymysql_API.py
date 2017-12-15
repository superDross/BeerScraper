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
                print(command)
                cursor.execute(command)
                self.db.commit()
            except Exception as e:
                logging.exception('message')
                self.db.rollback()
                self.close()
                sys.exit()


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
        self._valid_table()

    def _valid_table(self):
        ''' Raise an exception if table doesn't exist.'''
        command = "SELECT * FROM {}".format(self.table)
        with self.db.cursor() as cursor:
            cursor.execute(command)

    def dict2cmd(self, dictionary, command='insert'):
        ''' Construct a INSERT/USE SQL command from a dict, where
            the keys are column names in table and items are values.
        '''
        funcs = {'insert': self._insert_cmd,
                 'update': self._update_cmd}
        dictionary = collections.OrderedDict(dictionary)
        keys = [k for k, i in dictionary.items()]
        items = []
        for k, i in dictionary.items():
            if isinstance(i, int) or isinstance(i, float):
                i = str(i)
            elif not i:
                i = 'NULL'
            elif isinstance(i, str):
                i = "'{}'".format(i.replace("'", "").replace('"', ''))
            items.append(i)
        func = funcs.get(command)
        if not func:
            msg = '{} is not a valid command argument. Only {} are valid.'
            valid = ', '.join(funcs.keys())
            msg = msg.format(command, valid)
            raise custom_exceptions.InvalidChoice(
                command, valid, msg)
        sql_cmd = func(keys, items)
        return sql_cmd

    def _update_cmd(self, keys, items):
        equals = ['{} = {}'.format(k, i) for k, i in zip(keys, items)]
        command = 'UPDATE {} SET '.format(self.table) + ', '.join(equals)
        return command

    def _insert_cmd(self, keys, items):
        keys = ', '.join(keys)
        items = ', '.join(items)
        sql_cmd = 'INSERT INTO {} ({}) VALUES ({})'.format(
            self.table, keys, items)
        return sql_cmd

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
