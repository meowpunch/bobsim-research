from simulator.core import Simulator


def insert(name: str, sensitive: int, table):
    +table +".sql"
    path()
    a = (name,sensitive)

    print(type(a))
    print(str(a))

def main():
    simulator = Simulator()
    simulator.execute()

    query = "INSERT INTO "

    insert_item_table(name="경재", sensitive=0, table="item")



if __name__ == '__main__':
    main()
