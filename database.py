from beerhawk import get_all_products, extract_info, extract_beer_specs
from rate_beer import get_beer_info, get_brewery_info
from beerdb import get_all_beer_features
import time


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
for product in get_all_products()[15:20]:
    beerhawk_info = extract_info(product)
    beerhawk_specs = extract_beer_specs(beerhawk_info['Link'])
    beer = beerhawk_info['Beer']
    full_beer = '{} {}'.format(beerhawk_info['Brewery'], beer)
    print(full_beer)
    beerdb_info = get_all_beer_features(beer)
    if not beerdb_info:
        beerdb_info = get_all_beer_features(full_beer)

    ratebeer_info = get_beer_info(full_beer)
    # required as max requests is 1/second
    time.sleep(1)
    ratebeer_brewery = get_brewery_info(beerhawk_info['Brewery'])
    time.sleep(1)
    all_beerhawk_info = {**beerhawk_info, **beerhawk_specs}
    scrapped_data = [beerhawk_info, beerhawk_specs,
                     beerdb_info, ratebeer_info, ratebeer_brewery]
    beer_metadata = combine_dicts(scrapped_data)
    print(beer_metadata)
    print("\n")
