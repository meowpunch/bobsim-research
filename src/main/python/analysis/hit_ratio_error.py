def hit_ratio_error(err):
    tf = err.apply(lambda e: e < 0).value_counts()
    ratio = (tf / tf.sum() * 100).rename("hit ratio error")
    ratio.rename("hit ratio").plot(kind="bar", ylim=(ratio.min() * 0.9, ratio.max() * 1.1),
                                   title=ratio.name)
    return ratio

