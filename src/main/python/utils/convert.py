

class Converter:
    def __init__(self):
        pass

    def process(self):
        raise NotImplementedError

    @staticmethod
    def unit_by_person(data: int, person: int) -> float:
        if isinstance(data, int) or isinstance(person, int):
            raise TypeError
        return data/person
