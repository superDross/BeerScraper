''' Scraper functions to extract information from Beer Hawk.'''
import requests
import bs4
import re

BEER_HAWK = 'https://www.beerhawk.co.uk'


def get_url_text(link):
    ''' Download the url text from a given link.'''
    req = requests.get(link)
    req.raise_for_status()
    url = bs4.BeautifulSoup(req.text, features='lxml')
    return url


def get_all_products():
    ''' Get a list of all product HTML code as a list.'''
    link = BEER_HAWK + '/browse-beers?perPage=All'
    link_text = get_url_text(link)
    all_products = link_text.find_all('div', {'id': re.compile('product.*')})
    return all_products


def extract_info(product):
    ''' Extract info for a product.'''
    a_tag = product.find('a')
    a_info = ['data-brand', 'data-rating', 'data-sku', 'href']
    category, rating, sku, beer_link = [a_tag.get(x) for x in a_info]
    product_name = a_tag.find(
        'h3', {'class': "hidden-lg hidden-md hidden-sm"}).text
    price_span = a_tag.find('span', {'class': 'regular-price'})

    if price_span:
        price = price_span.text.replace('\n', '')
    else:
        price = None

    # Quite presumptious with the brewery var but seems to be the websites API
    beer_split = beer_link.replace("/", '').split("-")
    brewery = beer_split[0].title()

    if len(beer_split[1:]) == 1:
        beer_name = beer_split[1:][0].title()
    else:
        beer_name = ' '.join(beer_split[1:]).title()

    info_dict = {'Brewery': brewery, 'Beer': beer_name,
                 'Category': category, 'Rating': rating,
                 'Price': price, 'SKU': sku,
                 'Link': beer_link}
    return info_dict


def table2dict(table):
    ''' Converts a HTML table to a dict {col: row}.'''
    table_body = table.find('tbody')
    rows = table_body.find_all('tr')
    d = {}
    for row in rows:
        col_text = row.find('th').text
        row_text = row.find('td', {'class': 'data'}).text
        d[col_text] = row_text
    return d


def extract_beer_specs(sublink):
    ''' Extract beer specs from the given sublink.'''
    beer_page = get_url_text(BEER_HAWK + '/brewdog-punk-ipa')
    beer_specs = beer_page.find('table', id='product-attribute-specs-table')
    spec_dict = table2dict(beer_specs)
    return spec_dict
