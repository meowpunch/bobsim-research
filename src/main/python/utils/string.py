from functools import reduce

from utils.function import add


def get_digits_from_str(string: str) -> str:
    """
        TODO: mv to utils
        "a1b2c3" -> "123"
    """
    return reduce(add, filter(str.isdigit, string))


def get_float_from_str(string: str) -> float:
    """
        "1/3 T" -> 0.33..
        "10.5 g" -> 10.5
    """
    def isValid(s: str, d: str):
        sliced = s.split(d)
        if len(sliced) is not 2:
            raise ValueError
        return sliced

    if "/" in string:
        sliced = string.split("/")
        if len(sliced) is not 2:
            raise ValueError

        dividend, divisor = sliced[0], sliced[1]
        return dividend / divisor

    if "." in string:
        sliced =
        if len(digits) is not
    return 1.5
