from scrapers.beerhawk import BeerHawkProduct
from database.database_creation import open_database_table
from scrapers.rate_beer import get_info_dict
from datetime import datetime
import scrapers.brewerydb as brewerydb
import custom_exceptions
import logging
import unidecode
import re


def intiate_logger():
    logging.basicConfig(filename='beerscraper.log', level=logging.DEBUG)
    # ensure log prints to screen as well as writing to file
    logging.getLogger().addHandler(logging.StreamHandler())
    # ignores non-critical logging from urllib3 library
    logging.getLogger('urllib3').setLevel(logging.CRITICAL)
    now = datetime.now().strftime('%d/%m/%y %H:%M:%S')
    logging.info('Logger Intiated: {}'.format(now))


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
    brewerydb_info = brewerydb.get_all_beer_features(
        beer.beer_name, beer.brewery)
    if not brewerydb_info:
        msg = 'MISSING: {} not found in {} beer catalog'
        logging.warning(msg.format(beer.beer_name, beer.brewery))
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


def scrape_all_databases(beer):
    ''' Search and scrape BreweryDB and RateBeer for the parsed beer.'''
    brewerydb_info = scrape_brewerydb(beer)
    ratebeer_info = scrape_ratebeer(beer)
    scrapped_data = {**brewerydb_info, **
                     ratebeer_info, **beer.__dict__}
    combined_beer_info = clean_beer_dict(scrapped_data)
    return combined_beer_info


def scrape_all_products_info():
    ''' Scrape all beer hawk products information from
        beerhawk, brewerydb & ratebeer and insert it into
        a MySQL database tabel.
    '''
    intiate_logger()
    table = open_database_table()
    for product in get_all_beerhawk_products():
        try:
            beer = get_beerhawk_product(product)
            exists = table.exists('full_beer_name', beer.full_beer_name)
            if not exists:
                combined_beer_info = scrape_all_databases(beer)
                logging.info('ADDING: {} to database table'.format(
                    beer.full_beer_name))
                table.dict2cmd(combined_beer_info)
            else:
                msg = 'SKIPPING: {} already present in the database'
                logging.info(msg.format(beer.full_beer_name))
        except custom_exceptions.NonBeerProduct as e:
            logging.warning(
                'SKIPPING: detected non-beer product {}'.format(e.product))
            # logging.exception('message')
            continue


if __name__ == '__main__':
    scrape_all_products_info()
