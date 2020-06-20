import re
from functools import reduce

from utils.function import add, take


def get_digits_from_str(string: str) -> str:
    """
        TODO: mv to utils
        "a1b2c3" -> "123"
    """
    return reduce(add, filter(str.isdigit, string))


def get_float_from_str(string: str) -> float or None:
    """
        "1/3 T" -> 0.33..
        "10.5 g" -> 10.5

        Not implemented: 반컵 -> 0.5, 한컵 -> 1
    """
    regex_dict = {
        '/': r'\d/\d',
        '.': r'\d.\d',
        '': r'\d+'
    }
    key = take(1, filter(lambda k: k in string, regex_dict.keys()))[0]
    try:
        text = re.search(regex_dict[key], string).group()
        if key is '/':
            digits = text.split('/')
            return int(digits[0]) / int(digits[1])
        return float(text)
    except AttributeError:
        # TODO: handle 'NoneType Object has no attribute group'
        return None
