from beerhawk import BeerHawkProduct
from pymysql_API import DataBase
from rate_beer import get_info_dict
from beerdb import get_all_beer_features
import pymysql
import collections
import unidecode
import pandas as pd
import re


def get_all_beerhawk_products():
    ''' Get a list of all products HTML code.'''
    link = 'https://www.beerhawk.co.uk/browse-beers?perPage=All'
    link_text = BeerHawkProduct.get_url_text(link)
    all_products = link_text.find_all('div', {'id': re.compile('product.*')})
    return all_products


def combine_dicts(dicts):
    n = 0
    combined = {}
    for dictionary in dicts:
        if dictionary:
            n += len(dictionary)
            combined.update(dictionary)
    print('Total {}\tCombined {}'.format(n, len(combined)))
    return combined


def decode_dict_items(dictionary):
    ''' Decode and remove accents from dictionary items.'''
    for k, i in dictionary.items():
        if isinstance(i, str):
            dictionary[k] = unidecode.unidecode(i)
    return dictionary


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
        elif i is None:
            i = 'NULL'
        else:
            i = "'{}'".format(i)
        items.append(i)
    items = ', '.join(items)
    sql = 'INSERT INTO {} ({}) VALUES ({})'.format(table, keys, items)
    return sql


def open_database():
    ''' Open the beers database object and return.'''
    db = pymysql.connect(host='localhost', user='root',
                         password='Strokes_01!', db='beers')
    beer_db = DataBase(db)
    return beer_db


def do_it():
    beer_db = open_database()
    all_products = get_all_beerhawk_products()
    print('Processing {} products from beer hawk.'.format(len(all_products)))
    product_index = -1
    for product in all_products[5:]:
        product_index += 1
        beer = BeerHawkProduct(product)
        full_beer = '{} {}'.format(beer.brewery, beer.beer_name)
        print('PROCESSING: {}, index {}'.format(full_beer, product_index))
        exists = beer_db.exists('CRAFT_BEERS', 'beer_name', beer.beer_name)
        if not exists:
            beerdb_info = get_all_beer_features(beer.beer_name)
            if not beerdb_info:
                beerdb_info = get_all_beer_features(full_beer)
            ratebeer_info = get_info_dict(beer.beer_name, 'beers')
            scrapped_data = [beerdb_info, ratebeer_info, beer.__dict__]
            combined_beer_info = combine_dicts(scrapped_data)
            combined_beer_info = decode_dict_items(combined_beer_info)
            # unwanted keys
            combined_beer_info.pop('description', None)
            combined_beer_info.pop('tags', None)
            series = pd.Series(combined_beer_info)
            print(series)
            print("\n")
            sql = dict2sql(combined_beer_info, 'CRAFT_BEERS')
            print(sql)
            beer_db.cmd(sql)
            beer_db.print('SELECT beer_name FROM CRAFT_BEERS', 'CRAFT_BEERS')
        else:
            msg = 'SKIPPING: {} already present in the database'
            print(msg.format(full_beer))


if __name__ == '__main__':
    do_it()
