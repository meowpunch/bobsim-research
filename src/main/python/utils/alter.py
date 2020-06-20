

def alter_type_dict_to_list(data: dict, start_interval: int, end_interval: int):
    # dict.value() -> list
    iterator = list(data.values())[start_interval:end_interval]
    list_data = list(map(lambda x: x, iterator))
    return list_data


def alter_type_dict_item():
    pass


def alter_type_list_to_str(data: list, split=' '):
    str_data = split.join(map(str, data))
    return str_data


def remove_none(data: list):
    clean_data = list(filter(None.__ne__, data))
    return clean_data


def combine_sentence(sentence1, sentence2):
    combined = sentence1 + ' ' + sentence2
    return combined
