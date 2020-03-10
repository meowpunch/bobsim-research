from simulator.core import Simulator


def func(x):
    print(x[0])
    return x


def main():
    simulator = Simulator()
    simulator.execute()


if __name__ == '__main__':
    main()
