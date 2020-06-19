import os
from pathlib import Path


def get_source_root():
    return Path('C://BOBSIM/production-bobsim-python/')


def get_destination(destination):
    source_root = get_source_root()
    return source_root / destination


def main():
    print(get_source_root())


if __name__ == '__main__':
    main()
