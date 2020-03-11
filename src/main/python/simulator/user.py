from query_builder.core import InsertBuilder


class User:
    """
        Q)
            Is it right to make user class?
            If we make virtual user instants for simulation, it can be expensive (mem & time)
            What are the advantages of creating an instant.
            Is it worth it?

        TODO:
            define user behavior.
            Moreover, we can define behavior patterns. (schedule)

        - behavior type
        0. sign in
        1. login (landing)
        2. renew food materials in fridge (capture their fridge or receipt)
        3. search menus (we give recommended menu and get user's feedback)
        4. buy materials (we give recommended food materials and get user's feedback)

        - behavior pattern
        0. 0 -> 2 (sign in -> renew fridge)

    """

    def __init__(self, user_id=0):
        """
            TODO:
                if user_id is 0, new user. (create user)
                else, existing user. (select user)
                user's features for recommender system
        """
        self.id = user_id
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

    def capture_fridge(self, fridge_image):
        """
        TODO: update or insert virtual in user_item table
        """
        fridge = fridge_image()
        # iqb = InsertBuilder('user_table', fridge)
        # iqb.execute()

        print("\n\n------user's behavior------\n")
        print("ID: %d capture fridge" % self.id)
        print(fridge)
        return fridge

    def search_menu(self):
        # TODO: join btw user_item table & recipe_item table
        return print("ID: %d search menu" % self.id)

    def buy_materials(self):
        # TODO: i don't know
        return print("ID: %d buy materials" % self.id)
