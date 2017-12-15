from scrapers.beerhawk import BeerHawkProduct
from database.database_creation import open_database_table
from scrapers.rate_beer import get_info_dict
from datetime import datetime
import scrapers.brewerydb as brewerydb
import custom_exceptions
import logging
import unidecode
import argparse
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


def scrape_brewerydb(beer, brewery):
    ''' Scrape BreweryDB for a parsed BeerHawkProduct.'''
    logging.info('SCRAPING: BreweryDB....')
    brewerydb_info = brewerydb.get_all_beer_features(
        beer, brewery)
    if not brewerydb_info:
        msg = 'MISSING: {} not found in {} beer catalog'
        logging.warning(msg.format(beer, brewery))
        brewerydb_info = {}
    return brewerydb_info


def scrape_ratebeer(beer):
    ''' Scrape RateBeer for a parsed BeerHawkProduct.'''
    logging.info('SCRAPING: RateBeer....')
    ratebeer_info = get_info_dict(beer, 'beers')
    if not ratebeer_info:
        logging.warning('MISSING: No info found in RateBeer for {}'.format(
            beer))
        ratebeer_info = {}
    else:
        # don't want brewery objects
        ratebeer_info.pop('brewery', None)
    return ratebeer_info


def scrape_all_databases(beer):
    ''' Search and scrape BreweryDB and RateBeer for the parsed beer.'''
    brewerydb_info = scrape_brewerydb(beer.beer_name, beer.brewery)
    ratebeer_info = scrape_ratebeer(beer.full_beer_name)
    scrapped_data = {**brewerydb_info, **
                     ratebeer_info, **beer.__dict__}
    combined_beer_info = clean_beer_dict(scrapped_data)
    return combined_beer_info


def update_brewerydb():
    ''' Update BreweryDB entries in the CRAFT_BEERS table.'''
    intiate_logger()
    table = open_database_table()
    beers = table.column2list('full_beer_name')
    breweries = table.column2list('brewery')
    logging.info('Updating BreweryDB entries in the CRAFT_BEER table')
    for beer, brewery, in zip(beers, breweries):
        logging.info('-' * 85 + '\nUPDATING: {}'.format(beer))
        brewerydb_info = scrape_brewerydb(beer, brewery)
        brewerydb_info = clean_beer_dict(brewerydb_info)
        if brewerydb_info:
            logging.info(
                'ADDING: BreweryDB info for {} to the CRAFT_BEERS table'.format(beer))
            base_command = table.dict2cmd(brewerydb_info, 'update')
            condition = ' WHERE full_beer_name = "{}"'.format(beer)
            update_command = base_command + condition
            table.cmd(update_command)


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
                command = table.dict2cmd(combined_beer_info)
                table.cmd(command)
            else:
                msg = 'SKIPPING: {} already present in the database'
                logging.info(msg.format(beer.full_beer_name))
        except custom_exceptions.NonBeerProduct as e:
            logging.warning(
                'SKIPPING: detected non-beer product {}'.format(e.product))
            # logging.exception('message')
            continue


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Scrape all beers and meatadata from beerhawk, brewerydb and ratebeer')
    parser.add_argument('--brewerydb', '-b',
                        help='update brewerydb information in the database', action='store_true')
    args = vars(parser.parse_args())
    if args['brewerydb']:
        update_brewerydb()
    else:
        scrape_all_products_info()
