# https://www.tutorialspoint.com/python3/python_database_access.htm
from tabulate import tabulate


class DataBase(object):
    ''' Represents a SQL database.

    Parameters:
        db: a pymysql.connect() object

    Example:
        db = pymysql.connect(host='localhost', user='root',
                             password='pass', db='beers')
        sql = "INSERT INTO BEERS (NAME) VALUES ('Test Beer')"
        beers = DataBase(db)
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
            except:
                self.db.rollback()

    def print(self, command):
        ''' Print the command.'''
        with self.db.cursor() as cursor:
            cursor.execute(command)
            tupled_table = cursor.fetchall()
            cursor.execute('DESCRIBE BEERS')
            column_names = [x[0] for x in cursor.fetchall()]
            table = tabulate(tupled_table, column_names)
            print(table)

    def close(self):
        ''' Close the database object.'''
        self.db.close()
