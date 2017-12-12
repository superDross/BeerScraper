from beerhawk import BeerHawkProduct
from pymysql_API import SQLTable
from rate_beer import get_info_dict
from beerdb import get_all_beer_features
import custom_exceptions
import logging
import pymysql
import unidecode
import re


def intiate_logger():
    logging.basicConfig(filename='beerscraper.log', level=logging.DEBUG)
    # ensure log prints to screen as well as writing to file
    logging.getLogger().addHandler(logging.StreamHandler())
    # ignores non-critical logging from urllib3 library
    logging.getLogger('urllib3').setLevel(logging.CRITICAL)
    logging.info('Logger Intiated')


def create_table(db):
    ''' Create CRAFT_BEERS table in beers mySQL database.'''
    cmd = "CREATE TABLE CRAFT_BEERS(abv INT, abvMax INT, abvMin INT, beer_hawk VARCHAR(100), beer_link VARCHAR(100), full_beer_name VARCHAR(200) NOT NULL PRIMARY KEY, beer_name VARCHAR(100) NOT NULL, beer_style VARCHAR(25), bottle_size INT, brand_category VARCHAR(50), brewed_at VARCHAR(50), brewery VARCHAR(100), calories INT, country_origin VARCHAR(50), customer_rating FLOAT, fgMax FLOAT, fgMin FLOAT, ibu FLOAT, ibuMax FLOAT, ibuMin FLOAT, img_url VARCHAR(200), isOrganic VARCHAR(5), mean_rating FLOAT, name VARCHAR(200), num_ratings INT, overall_rating FLOAT, price FLOAT, retired VARCHAR(10), seasonal VARCHAR(20), serving_temp VARCHAR(20), sku VARCHAR(20), srm INT, style VARCHAR(50), style_rating INT, style_url VARCHAR(100), url VARCHAR(200), weighted_avg FLOAT, _has_fetched VARCHAR(10))"
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
    # don't want brewery objects
    combined_beer_info.pop('brewed_at', None)
    return combined_beer_info


def get_beerhawk_product(product):
    ''' Create a BeerHawkProduct object.'''
    logging.info('-' * 85 + '\nSCRAPING: BeerHawk')
    beer = BeerHawkProduct(product)
    logging.info('PROCESSING: {} (beer: {}, brewery: {})'.format(
        beer.full_beer_name, beer.beer_name, beer.brewery))
    return beer


def scrape_brewerydb(beer):
    ''' Scrape BreweryDB for a parsed BeerHawkProduct.'''
    logging.info('SCRAPING: BreweryDB....')
    brewerydb_info = get_all_beer_features(beer.full_beer_name)
    if not brewerydb_info:
        logging.warning('MISSING: No info found in BreweryDB for {}'.format(
            beer.full_beer_name))
        brewerydb_info = {}
    return brewerydb_info


def scrape_ratebeer(beer):
    ''' Scrape RateBeer for a parsed BeerHawkProduct.'''
    logging.info('SCRAPING: RateBeer....')
    ratebeer_info = get_info_dict(beer.full_beer_name, 'beers')
    if not ratebeer_info:
        logging.warning('MISSING: No info found in RateBeer for {}'.format(
            beer.full_beer_name))
        ratebeer_info = {}
    else:
        # don't want brewery objects
        ratebeer_info.pop('brewery', None)
    return ratebeer_info


def scrape_all_products_info():
    ''' Scrape all beer hawk products information from
        beerhawk, beerdb & ratebeer and insert it into
        a MySQL database tabel.
    '''
    intiate_logger()
    table = open_database_table()
    for product in get_all_beerhawk_products():
        try:
            beer = get_beerhawk_product(product)
            exists = table.exists('full_beer_name', beer.full_beer_name)
            if not exists:
                brewerydb_info = scrape_brewerydb(beer)
                ratebeer_info = scrape_ratebeer(beer)
                scrapped_data = {**brewerydb_info, **
                                 ratebeer_info, **beer.__dict__}
                combined_beer_info = clean_beer_dict(scrapped_data)
                logging.info('ADDING: {} to database table'.format(
                    beer.full_beer_name))
                table.dict2cmd(combined_beer_info)
            else:
                msg = 'SKIPPING: {} already present in the database'
                logging.info(msg.format(beer.full_beer_name))
        except custom_exceptions.NonBeerProduct as e:
            logging.warning(
                'SKIPPING: detected non-beer product {}'.format(e.product))
            continue


if __name__ == '__main__':
    scrape_all_products_info()
