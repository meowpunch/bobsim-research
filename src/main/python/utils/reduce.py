from functools import reduce


def combine_list(t_list: list):
    return reduce(lambda x, y: x + y, t_list)


def main():
    print(combine_list([1, 2]))


if __name__ == '__main__':
    main()
