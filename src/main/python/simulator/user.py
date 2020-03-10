class User:
    """
        TODO: define user behavior

        0. sign in

        1. login (landing)

        2. renew food materials in fridge
        (capture their fridge or receipt)

        3. search menus
        (we give recommended menu and get user's feedback)

        4. buy materials
        (we give recommended food materials and get user's feedback)

    """

    def __init__(self):
        # TODO: user's features for recommender system
        self.id = -1
        self.b_type = 0  # driven from user's behavior

    # def process(self):
    #     # TODO: user's behavior process not like below sequentially.
    #     self.sign_in()
    #     self.login()
    #
    #     self.renew_fridge()
    #     self.search_menu()
    #     self.buy_materials()
    #     return

    def sign_in(self):
        # TODO: insert data in user table(RDS)
        return print("ID: %d sign in" % self.id)

    def login(self):
        # TODO: select data in user table
        return print("ID: %d login" % self.id)

    def renew_fridge(self):
        # TODO: update or insert data in user_item table
        return print("ID: %d renew fridge" % self.id)

    def search_menu(self):
        # TODO: join btw user_item table & recipe_item table
        return print("ID: %d search menu" % self.id)

    def buy_materials(self):
        # TODO: i don't know
        return print("ID: %d buy materials" % self.id)
