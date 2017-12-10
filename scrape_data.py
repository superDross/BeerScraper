from beerhawk import BeerHawkProduct
from pymysql_API import DataBase
from rate_beer import get_info_dict
from beerdb import get_all_beer_features
import logging
import pymysql
import unidecode
import re


def open_database():
    ''' Open the beers database object and return.'''
    db = pymysql.connect(host='localhost', user='root',
                         password='Strokes_01!', db='beers',
                         use_unicode=True, charset='utf8')
    beer_db = DataBase(db)
    return beer_db


def get_all_beerhawk_products():
    ''' Get a list of all beerhawk products HTML code.'''
    link = 'https://www.beerhawk.co.uk/browse-beers?perPage=All'
    link_text = BeerHawkProduct.get_url_text(link)
    all_products = link_text.find_all('div', {'id': re.compile('product.*')})
    return all_products


def decode_dict_items(dictionary):
    ''' Decode and remove accents from dictionary items.'''
    for k, i in dictionary.items():
        if isinstance(i, str):
            i = i.replace("\\u", "")
            dictionary[k] = unidecode.unidecode(i)
    return dictionary


def clean_beer_dict(dictionary):
    ''' Remove unwnated keys and replace unwanted
        characters in item strings.
    '''
    combined_beer_info = decode_dict_items(dictionary)
    combined_beer_info.pop('description', None)
    combined_beer_info.pop('tags', None)
    return combined_beer_info


def scrape_all_products_info():
    ''' Scrape all beer hawk products information from
        beerhawk, beerdb & ratebeer and insert it into
        a MySQL database tabel.
    '''
    logging.basicConfig(filename='beerscraper.log', level=logging.DEBUG)
    logging.info('Intiated')
    beer_db = open_database()
    for product in get_all_beerhawk_products()[6:]:
        print('scraping beerhawk')
        logging.info('scraping beerhawk')
        beer = BeerHawkProduct(product)
        logging.info('processing {}'.format(beer.full_beer_name))
        print('PROCESSING: {} (beer: {}, brewery: {})'.format(
            beer.full_beer_name, beer.beer_name, beer.brewery))
        exists = beer_db.exists('CRAFT_BEERS', 'full_beer_name',
                                beer.full_beer_name)
        if not exists:
            print('scraping beerdb')
            logging.info('scraping beerdb')
            beerdb_info = get_all_beer_features(beer.beer_name)
            if not beerdb_info:
                beerdb_info = get_all_beer_features(beer.full_beer_name)
                if not beerdb_info:
                    print('No info found in beerdb for {}'.format(
                        beer.full_beer_name))
                    beerdb_info = {}
            # print(beerdb_info)
            print('scraping ratebeer')
            logging.info('scrapping ratebeer')
            ratebeer_info = get_info_dict(beer.beer_name, 'beers')
            if not ratebeer_info:
                print('No info found in rate beer for {}'.format(
                    beer.full_beer_name))
                ratebeer_info = {}
            # print(ratebeer_info)
            scrapped_data = {**beerdb_info, **ratebeer_info, **beer.__dict__}
            combined_beer_info = clean_beer_dict(scrapped_data)
            print('adding to database')
            logging.info('adding to database')
            beer_db.dict2cmd(combined_beer_info, 'CRAFT_BEERS')
        else:
            msg = 'SKIPPING: {} already present in the database'
            print(msg.format(beer.full_beer_name))
            logging.info(msg.format(beer.full_beer_name))


if __name__ == '__main__':
    scrape_all_products_info()
