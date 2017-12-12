import custom_exceptions
import requests
import bs4


class BeerHawkProduct(object):
    ''' Container for product details scrapped from beer hawk.

    Parameters:
        product: div element (id=product*) from beerhawk browse-beer page
    '''

    def __init__(self, product):
        self.beer_hawk = 'https://www.beerhawk.co.uk'
        self.product = product.find('a')
        self.price = self._get_beer_price()
        self.brand_category = self.product.get('data-brand')
        self.customer_rating = self.product.get('data-rating')
        self.sku = self.product.get('data-sku')
        self.beer_link = self.product.get('href')
        self.brewery = self._get_brewery_name()
        self.beer_name = self._get_beer_name()
        self.dict = self.extract_beer_specs()
        self.abv = float(self.dict.get('ABV').replace("%", ''))
        self.bottle_size = int(self.dict.get('Bottle Size').replace('ml', ''))
        self.country_origin = self.dict.get('Country')
        self.serving_temp = self.dict.get('Serving Temp')
        self.beer_style = self.dict.get('Style')
        self.full_beer_name = '{} {}'.format(self.brewery, self.beer_name)
        # Attributes no longer needed are deleted
        del self.dict
        del self.product

    def _get_beer_price(self):
        # print(self.product)
        price_span = self.product.find('span', {'class': 'regular-price'})
        if not price_span:
            price_span = self.product.find('span', {'class': 'old-price'})
        if price_span:
            price = price_span.text.replace('\n', '')
            price = float(price.replace("£", ''))
        else:
            price = None
        return price

    def _get_brewery_name(self):
        beer_split = self.beer_link.replace(
            "/", '').replace('brewery-', '').split("-")
        # the indexing here is quite presumptious
        brewery = beer_split[0].title()
        return brewery

    def _get_beer_name(self):
        beer_split = self.beer_link.replace(
            "/", '').replace('brewery-', '').split("-")
        if len(beer_split[1:]) == 1:
            beer_name = beer_split[1:][0].title()
        else:
            beer_name = ' '.join(beer_split[1:]).title()
        return beer_name

    @staticmethod
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

    def extract_beer_specs(self):
        ''' Extract beer specs from the given product sublink HTML table.'''
        beer_page = self.get_url_text(self.beer_hawk + self.beer_link)
        if 'gift' in beer_page.find('title').text.lower():
            raise custom_exceptions.NonBeerProduct(self.beer_link)
        try:
            beer_specs = beer_page.find(
                'table', id='product-attribute-specs-table')
            spec_dict = self.table2dict(beer_specs)
            return spec_dict
        except AttributeError:
            msg = '{} does not contain a product spec table'
            raise custom_exceptions(self.beer_link, msg.format(self.beer_link))

    @staticmethod
    def get_url_text(link):
        ''' Download the url text from a given link.'''
        req = requests.get(link)
        req.raise_for_status()
        url = bs4.BeautifulSoup(req.text, features='lxml')
        return url
