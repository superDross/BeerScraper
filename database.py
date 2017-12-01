from beerhawk_product import BeerHawkProduct
from rate_beer import get_info_dict
from beerdb import get_all_beer_features
import pandas as pd
import re


def get_all_products():
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


# Generators may be better
def do_it():
    all_products = get_all_products()[15:20]
    print('Processing {} products from beer hawk.'.format(len(all_products)))
    all_series = []
    for product in all_products:
        beer = BeerHawkProduct(product)
        full_beer = '{} {}'.format(beer.brewery, beer.beer_name)
        print(full_beer)
        beerdb_info = get_all_beer_features(beer.beer_name)
        if not beerdb_info:
            beerdb_info = get_all_beer_features(full_beer)
        ratebeer_info = get_info_dict(beer.beer_name, 'beers')
        scrapped_data = [beer.__dict__, beerdb_info, ratebeer_info]
        combined_beer_info = combine_dicts(scrapped_data)
        series = pd.Series(combined_beer_info)
        all_series.append(series)
        print(series)
        print("\n")

    df = pd.concat(all_series, axis=1).T
    df.to_csv('test.csv', sep="\t")
    #    beerhawk_info = extract_info(product)
    #    beerhawk_specs = extract_beer_specs(beerhawk_info['Link'])
    #    beer = beerhawk_info['Beer']
    #    full_beer = '{} {}'.format(beerhawk_info['Brewery'], beer)
    #    print(full_beer)
    #    beerdb_info = get_all_beer_features(beer)
    #    if not beerdb_info:
    #        beerdb_info = get_all_beer_features(full_beer)
    #
    #    ratebeer_info = get_info_dict(full_beer, 'beers')
    #    # required as max requests is 1/second
    #    time.sleep(1)
    #    # ratebeer_brewery = get_brewery_info(beerhawk_info['Brewery'])
    #    ratebeer_brewery = get_info_dict(beerhawk_info['Brewery'], 'breweries')
    #    time.sleep(1)
    #    all_beerhawk_info = {**beerhawk_info, **beerhawk_specs}
    #    scrapped_data = [beerhawk_info, beerhawk_specs,
    #                     beerdb_info, ratebeer_info, ratebeer_brewery]
    #    beer_metadata = combine_dicts(scrapped_data)
    #    print(beer_metadata)
    #    print("\n")


if __name__ == '__main__':
    do_it()
