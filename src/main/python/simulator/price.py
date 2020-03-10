""""

    1.
"""

from scipy.stats import truncnorm


def get_truncated_normal(mean=0, sd=1, low=0, upp=10):
    return truncnorm(
        (low - mean) / sd, (upp - mean) / sd, loc=mean, scale=sd)


def price(num, avg, delta, d_type=None):
    print("let's price")
    mean, sigma = float(avg), delta*0.5
    # x_price = np.array([(0 if q is 0 else ) for q in x_quantity])

    x = get_truncated_normal(mean=mean, sd=sigma, low=mean - delta, upp=mean + delta)
    x = x.rvs(num)

    # TODO: make visualize.py class form.
    # visualize.plot(data=[x])

    # for checking
    # print(x.round().astype(int))

    return x.round().astype(int)
