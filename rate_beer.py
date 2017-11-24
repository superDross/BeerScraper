from fuzzywuzzy import fuzz
import ratebeer

RB = ratebeer.RateBeer()


def query_ratebeer(query):
    results = RB.search(query)
    return results


def fuzzy_query_match(query, beers):
    pr = []
    for beer in beers:
        compare = fuzz.partial_ratio(query.lower(), beer.name.lower())
        pr.append(compare)
    index = pr.index(max(pr))
    best_match = beers[index]
    return best_match


def fetch_data(data, key):
    fetched_data = []
    breweries = data.get(key)
    for x in breweries:

        brewery = RB.get_brewery(x.url, fetch=True)
        fetched_data.append(brewery)
    return fetched_data


def get_beer_info(query):
    try:
        search_results = query_ratebeer(query)
        beers = search_results.get('beers')
        if beers:
            # indexing here is VERY presumptious
            # dict to stop AttributeError occuring and instead to return None
            if len(beers) > 1 or not all(x.name is None for x in beers):
                beer_match = fuzzy_query_match(query, beers)
            beer_url = beer_match.url
            beer = RB.get_beer(beer_url, fetch=True).__dict__

            # print(beer)
            ratebeer_dict = {'beerrate_overall': beer.get('overall_rating'),
                             'beerrate_number': beer.get('num_ratings'),
                             'style_rating': beer.get('style_rating'),
                             'beerrate_style': beer.get('style'),
                             'beerrate_name': beer.get('name'),
                             'calories': beer.get('calories'),
                             'beerrate_ibu': beer.get('ibu'),
                             'beerrate_abv': beer.get('abv')}
            # return ratebeer_dict
            return beer
        else:
            print('WARNING: no beer info for {}'.format(query))
    except ratebeer.rb_exceptions.PageNotFound:
        print('WARNING: no beer info for {}'.format(query))
        return None


def get_brewery_info(query):
    try:
        search_results = query_ratebeer(query)
        breweries = fetch_data(search_results, 'breweries')
        if breweries:
            # indexing is VERY presumptious
            if len(breweries) > 1 or not all(x.name is None for x in breweries):
                brewery_match = fuzzy_query_match(query, breweries)
            else:
                brewery_match = breweries[0]
            brewery = brewery_match.__dict__
            # print(brewery)
            brewery_dict = {'city': brewery.get('city'),
                            'country': brewery.get('country'),
                            'state': brewery.get('state'),
                            'location': brewery.get('location'),
                            'brewery_name': brewery.get('name'),
                            'brewery_type': brewery.get('type')}
            # return brewery_dict
            return brewery
        else:
            print('WARNING: no brewery info for {}'.format(query))
    except ratebeer.rb_exceptions.PageNotFound:
        print('WARNING: no brewery info for {}'.format(query))
        return None
