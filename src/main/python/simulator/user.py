from query_builder.core import InsertBuilder
from simulator.menu import cost_menu


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
        self.gender = 3
        self.b_type = 2  # type of behavior
        self.fridge = None
        self.menu = None
        # TODO: diversify static features.
        # static is declared before dynamic
        self.record = dict({
            "id": self.id,
            "gender": self.gender,
            "driven": self.b_type,
        })

    """
        By type of behavior
    """
    def sign_in(self):
        # TODO: insert data in user table(RDS)
        print("ID: %d sign in" % self.id)
        pass

    def login(self):
        # TODO: select data in user table
        print("ID: %d login" % self.id)
        pass

    def capture_fridge(self, fridge_image):
        """
        TODO:
            update or insert virtual in user_item table
            change location of cost_menu function.
        """
        self.fridge = fridge_image()
        self.menu = cost_menu(self.fridge)
        # iqb = InsertBuilder('user_table', fridge)
        # iqb.execute()

        print("\n\n------user's behavior------\n")
        print("ID: %d capture fridge" % self.id)
        print(self.fridge)
        print("\nprobable menu & cost")
        print(self.menu)
        return self.fridge

    def search_menu(self):
        # TODO: join btw user_item table & recipe_item table
        print("ID: %d search menu" % self.id)
        pass

    def buy_materials(self):
        # TODO: i don't know
        print("ID: %d buy materials" % self.id)
        pass
