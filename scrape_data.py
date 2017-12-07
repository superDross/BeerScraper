from beerhawk import BeerHawkProduct
from pymysql_API import DataBase
from rate_beer import get_info_dict
from beerdb import get_all_beer_features
import pymysql
import unidecode
import re


def open_database():
    ''' Open the beers database object and return.'''
    db = pymysql.connect(host='localhost', user='root',
                         password='Strokes_01!', db='beers')
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
    beer_db = open_database()
    for product in get_all_beerhawk_products()[35:]:
        beer = BeerHawkProduct(product)
        full_beer = '{} {}'.format(beer.brewery, beer.beer_name)
        print('PROCESSING: {}'.format(full_beer))
        exists = beer_db.exists('CRAFT_BEERS', 'beer_name', beer.beer_name)
        if not exists:
            beerdb_info = get_all_beer_features(beer.beer_name)
            if not beerdb_info:
                beerdb_info = get_all_beer_features(full_beer)
            ratebeer_info = get_info_dict(beer.beer_name, 'beers')
            scrapped_data = {**beerdb_info, **ratebeer_info, **beer.__dict__}
            combined_beer_info = clean_beer_dict(scrapped_data)
            beer_db.dict2cmd(combined_beer_info, 'CRAFT_BEERS')
        else:
            msg = 'SKIPPING: {} already present in the database'
            print(msg.format(full_beer))


if __name__ == '__main__':
    scrape_all_products_info()
