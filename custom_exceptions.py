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
