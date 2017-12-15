''' Scrapers functions to extract information from the BreweryDB.'''
from fuzzywuzzy import fuzz
import scrapers.APIkeys
import requests
import json

# Below example gets all beer from Edinburgh, Scotland.
# r = 'locations?region=Scotland&locality=edinburgh'
# brewaries = get_data_request(r, key).get('data')

KEYS = scrapers.APIkeys.keys.get('BreweryDB')


def get_json_data(link):
    ''' Download the json text from a given link.'''
    req = requests.get(link)
    req.raise_for_status()
    data = json.loads(req.text)
    return data


def get_data_request(request, key_num=0):
    ''' Request something from the BreweryDB.'''
    try:
        db = 'https://api.brewerydb.com/v2/'
        data = get_json_data('{}{}&key={}'.format(db, request, KEYS[key_num]))
        return data
    except requests.exceptions.HTTPError as e:
        # use another API key
        key_num += 1
        return get_data_request(request, key_num)


def get_beer_data(beer):
    ''' Get all JSON data for a given beer.'''
    r = 'beers?name={}'.format(beer)
    # only returns search result if their is an exact match
    # only premium users can use wildcards for searching
    search_results = get_data_request(r)
    return search_results


def get_brewery_data(brewery):
    ''' Get all data for a given brewery name.'''
    r = 'breweries?name={}'.format(brewery)
    # only returns search result if their is an exact match
    # only premium users can use wildcards for searching
    search_results = get_data_request(r)
    if search_results.get('data'):
        # The index assumes only one brewery was returned from search query
        brewery_data = search_results.get('data')[0]
        return brewery_data


def all_beers_from_brewery(brewery):
    ''' Return a list of all beers for a given brewery name.'''
    brewery_data = get_brewery_data(brewery)
    if brewery_data:
        brewery_id = brewery_data.get('id')
        r = 'brewery/{}/beers?'.format(brewery_id)
        beers = get_data_request(r).get('data')
        return beers


def fuzzy_query_match(query, beers, min_match=95):
    ''' Find the best partial match between a
        search query and beer name and return
        the beer name if it meets the given
        minimum partial match percentage.
    '''
    pr = []
    all_beer_names = [x.get('name') for x in beers]
    for beer in all_beer_names:
        compare = fuzz.partial_ratio(query.lower(), beer.lower())
        pr.append(compare)
    if max(pr) >= min_match:
        index = pr.index(max(pr))
        best_match = beers[index]
        return best_match


def check_beer_in_brewery_catalog(beer, brewery):
    ''' Determine whether a beer is within a brewery catalog
        by using fuzzy partial matching against all brewery beers..
        Return beer JSON if True.
    '''
    brewery_beers = all_beers_from_brewery(brewery)
    if brewery_beers:
        beer_data = fuzzy_query_match(beer, brewery_beers)
        return beer_data


def filter_dict(dictionary, keys):
    ''' Filter dictionary for given keys.'''
    f_dict = {}
    for index, key in enumerate(keys):
        key_data = dictionary.get(key)
        if key_data and key == 'srm':
            key_data = key_data.get('id')
        f_dict[keys[index]] = key_data
    return f_dict


def filter4data(beer_data):
    ''' Filter the JSON dict for features of interest and return.'''
    # The indexing maybe presumtious here
    beer_data = beer_data[0] if isinstance(beer_data, list) else beer_data
    features = ['ibu', 'isOrganic', 'abv', 'srm']
    features1 = filter_dict(beer_data, features)
    style_features = ['abvMin', 'abvMax', 'fgMin',
                      'fgMax', 'ibuMin', 'ibuMax', 'name']
    if beer_data.get('style'):
        features2 = filter_dict(beer_data.get('style'), style_features)
        beer_features = {**features1, **features2}
        return beer_features
    else:
        return features1


def get_all_beer_features(beer, brewery):
    ''' Extract all features of interest for a given beer
        from BreweryDB and return as a dict.
    '''
    beer_data = check_beer_in_brewery_catalog(beer, brewery)
    if beer_data:
        beer_dict = filter4data(beer_data)
        return beer_dict
