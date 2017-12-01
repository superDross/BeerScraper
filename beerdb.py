''' Scrapers functions to extract information from the BeerDB.'''
import APIkeys
import requests
import json

# Below example gets all beer from Edinburgh, Scotland.
# r = 'locations?region=Scotland&locality=edinburgh'
# brewaries = get_data_request(r, key).get('data')

KEY = APIkeys.keys.get('BreweryDB')


def get_json_data(link):
    ''' Download the json text from a given link.'''
    req = requests.get(link)
    req.raise_for_status()
    data = json.loads(req.text)
    return data


def get_data_request(request):
    db = 'https://api.brewerydb.com/v2/'
    data = get_json_data('{}{}&key={}'.format(db, request, KEY))
    return data


def get_brewery_data(brewery):
    ''' Get all data for a given brewery name.'''
    r = 'breweries?name={}'.format(brewery)
    search_results = get_data_request(r)
    # The index assumes only one brewery was returned from search query
    brewery_data = search_results.get('data')[0]
    return brewery_data


def all_beers_from_brewery(brewery):
    ''' Return a list of all beers for a given brewery name.'''
    brewery_data = get_brewery_data(brewery)
    brewery_id = brewery_data.get('id')
    r = 'brewery/{}/beers?'.format(brewery_id)
    beers = get_data_request(r).get('data')
    return beers


def get_beer_data(beer):
    ''' Get all JSON data for a given beer.'''
    r = 'beers?name={}'.format(beer)
    search_results = get_data_request(r)
    return search_results


def filter_dict(dictionary, keys):
    ''' Filter dictionary for given keys.'''
    f_dict = {}
    for index, key in enumerate(keys):
        key_data = dictionary.get(key)
        if key_data and key == 'srm':
            key_data = key_data.get('id')
        f_dict[keys[index]] = key_data
    return f_dict


def get_all_beer_features(beer):
    ''' Extract all features of interest for a given beer
        from BeerDB and return as a dict.
    '''
    beer_data = get_beer_data(beer).get('data')
    if beer_data:
        # The indexing maybe presumtious here
        beer_data = beer_data[0]
        features = ['ibu', 'isOrganic', 'abv', 'srm']
        features1 = filter_dict(beer_data, features)
        style_features = ['abvMin', 'abvMax', 'fgMin',
                          'fgMax', 'ibuMin', 'ibuMax', 'name']
        features2 = filter_dict(beer_data.get('style'), style_features)
        beer_features = {**features1, **features2}
        return beer_features
