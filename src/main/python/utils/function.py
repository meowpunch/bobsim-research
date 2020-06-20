from functools import reduce


# For functional programming
def take(length, iterator):
    res = []
    for e in iterator:
        res.append(e)
        if len(res) == length:
            return res
    return res


def add(x, y):
    return x + y

