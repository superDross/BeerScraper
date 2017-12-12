''' Custom exceptions used throughout the package.'''


class InvalidChoice(Exception):
    ''' Raise if an unwanted string has been parsed.'''

    def __init__(self, choice, valid_choices, msg=None):
        if not msg:
            valid = ', '.join(valid_choices)
            s = '{} is not a valid choice. Only {} are valid'
            msg = s.format(choice, valid)
        Exception.__init__(self, msg)
        self.choice = choice
        self.valid_choices = valid_choices
        self.msg = msg


class NonBeerProduct(Exception):
    ''' Raise if a product is not a beer.'''

    def __init__(self, product, msg=None):
        if not msg:
            msg = '{} is not a beer product'.format(product)
        Exception.__init__(self, msg)
        self.product = product
        self.msg = msg
