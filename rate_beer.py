''' Functions to query ratebeer.com'''
from fuzzywuzzy import fuzz
import custom_exceptions
import logging
import ratebeer
import time

RB = ratebeer.RateBeer()


def query_ratebeer(query):
    ''' Search ratebeer for a given query.'''
    results = RB.search(query)
    return results


def fuzzy_query_match(query, beers):
    ''' Find the best partial match between a
        search query and beer object and return
        the beer object.
    '''
    pr = []
    for beer in beers:
        compare = fuzz.partial_ratio(query.lower(), beer.name.lower())
        pr.append(compare)
    index = pr.index(max(pr))
    best_match = beers[index]
    return best_match


def check_choice_error(choice):
    ''' Raise an error if choice is not beers or breweries.'''
    valid = ['beers', 'breweries']
    if choice not in valid:
        raise custom_exceptions.InvalidChoice(choice, valid)


def fetch_data(data, key):
    ''' Fetch the search results from ratebeer.
    Args:
        data: ratebeer search results
        key: 'beers' or 'breweries'
    '''
    time.sleep(1)
    check_choice_error(key)
    breweries = data.get(key)
    fetched_data = []
    if breweries:
        func = RB.get_beer if key == 'beers' else RB.get_brewery
        for x in breweries:
            # fetch boolean required to ensure all data is obtained
            brewery = func(x.url, fetch=True)
            fetched_data.append(brewery)
    return fetched_data


def get_info_dict(query, choice):
    ''' Search rate beer for a beer or brewery query
        and return the best matched objects attributes.
    Args:
        query: search query
        choice: 'beers' or 'breweries'
    '''
    try:
        check_choice_error(choice)
        search_results = query_ratebeer(query)
        # ensure at least 1 second between requests to ratebeer
        beers = fetch_data(search_results, choice)
        if beers:
            if len(beers) > 1 or not all(x.name is None for x in beers):
                beer_match = fuzzy_query_match(query, beers)
            else:
                beer_match = beers[0]
            beer = beer_match.__dict__
            return beer
        else:
            print('WARNING: no {} info for {}'.format(choice, query))
            logging.warning('no {} info for {}'.format(choice, query))
    except ratebeer.rb_exceptions.PageNotFound:
        print('WARNING: no {} info for {}'.format(choice, query))
        logging.warning('no {} info for {}'.format(choice, query))


#
# def get_all_info(query_beer, query_brewery):
#     full_beer = '{} {}'.format(query_brewery, query_beer)
#     time.sleep(1)
#     brewery_info = get_info_dict(query_brewery, 'breweries')
#     # sleep required to
#     time.sleep(1)
#     beer_info = get_info_dict(full_beer, 'beers')
#     from database import combine_dicts
#     all_beer_info = combine_dicts([beer_info, brewery_info])
#     return all_beer_info
