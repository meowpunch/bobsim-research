def get_unit(unit_name):
    return {
        '20KG': 200, '1.2KG': 12, '8KG': 80, '5KG': 5, '2KG': 2, '1KG': 10, '1KG(단)': 10, '1KG(1단)': 10,
        '600G': 6, '500G': 5, '200G': 2, '100G': 1,
        '10마리': 10, '5마리': 5, '2마리': 2, '1마리': 1,
        '30개': 10, '10개': 10, '1개': 1,
        '1L': 10,
        '1속': 1,
        # TODO: handle no supported unit
    }.get(unit_name, 1)