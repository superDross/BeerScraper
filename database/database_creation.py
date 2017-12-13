from database.pymysql_API import SQLTable
import logging
import pymysql


def create_table(db):
    ''' Create CRAFT_BEERS table in beers mySQL database.'''
    cmd = "CREATE TABLE CRAFT_BEERS(abv INT, abvMax INT, abvMin INT, beer_hawk VARCHAR(100), beer_link VARCHAR(100), full_beer_name VARCHAR(200) NOT NULL PRIMARY KEY, beer_name VARCHAR(100) NOT NULL, beer_style VARCHAR(25), bottle_size INT, brand_category VARCHAR(50), brewed_at VARCHAR(50), brewery VARCHAR(100), calories INT, country_origin VARCHAR(50), customer_rating FLOAT, fgMax FLOAT, fgMin FLOAT, ibu FLOAT, ibuMax FLOAT, ibuMin FLOAT, img_url VARCHAR(200), isOrganic VARCHAR(5), mean_rating FLOAT, name VARCHAR(200), num_ratings INT, overall_rating FLOAT, price FLOAT, retired VARCHAR(10), seasonal VARCHAR(20), serving_temp VARCHAR(200), sku VARCHAR(20), srm INT, style VARCHAR(50), style_rating INT, style_url VARCHAR(100), url VARCHAR(200), weighted_avg FLOAT, _has_fetched VARCHAR(10))"
    db.cursor().execute(cmd)


def create_database():
    ''' Create a beers mySQL database.'''
    db = pymysql.connect(host='localhost', user='root',
                         password='database', use_unicode=True,
                         charset='utf8')
    command = 'CREATE DATABASE beers'
    db.cursor().execute(command)
    create_table(db)


def open_database_table():
    ''' Open the beers database object and return.'''
    try:
        db = pymysql.connect(host='localhost', user='root',
                             password='database', db='beers',
                             use_unicode=True, charset='utf8')
        table = SQLTable(db, 'CRAFT_BEERS')
        return table
    except pymysql.err.InternalError as e:
        logging.info('Database beers does not exist. Creating...')
        create_database()
    except pymysql.err.ProgrammingError as e:
        logging.info('Table CRAFT_BEERS does not exist. creating...')
        create_table()
