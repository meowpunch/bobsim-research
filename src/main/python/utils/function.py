
def take(length, iterator):
    # For functional programming
    res = []
    for e in iterator:
        res.append(e)
        if len(res) == length:
            return res
    return res
