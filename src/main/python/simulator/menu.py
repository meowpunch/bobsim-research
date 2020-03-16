import pandas as pd

from query_builder.core import SelectBuilder


def mask_by_count(item_count, recipe_cost, threshold=5):
    """
        TODO:
            ABOUT THRESHOLD
            for user's convenience,
            the number of ingredients that user doesn't have
            for recipe's diversity,
            proportion of ingredients that user doesn't have

            Q) which will affect users more.
    """
    rcc = pd.merge(item_count, recipe_cost)
    mask = rcc.apply(lambda x: x.item_count < threshold, axis=1)
    return rcc[mask]


def join_query(recipe_item, user_item):
    """
        TODO: if we store virtual user's data in db, this is replaced by queryBuilder

        SELECT *
        FROM recipe_item
        LEFT JOIN user_item
        ON recipe_item.item_id = user_item.item_id
    """
    print(pd.merge(
        left=recipe_item, right=user_item,
        how='left', left_on='item_id', right_on='id'
    ))
    return pd.merge(
        left=recipe_item, right=user_item,
        how='left', left_on='item_id', right_on='id'
    )


def load_recipe_item():
    sqb = SelectBuilder(table_name="recipe_item", att_name="recipe_id, item_id", where_clause="")
    return sqb.execute()


def load_recipe():
    """
        if we only need recipe_id, abandon this func
    """
    sqb = SelectBuilder(table_name="recipe", att_name="id, name, season_id")
    return sqb.execute()


# core function
def cost_menu(fridge: pd.core.frame.DataFrame):
    """
        TODO: After we implement queryBuilder, code will be more cleaned

            1. load recipe_item table
            2. select possible menu from recipe_item table.
            3. calculate menu's cost
            2&3 is mixed

        regard 'recipe' as 'menu'
    """
    recipe_corpus = load_recipe_item()

    recipe_group = join_query(recipe_corpus, fridge).groupby("recipe_id")

    # a = fridge.apply(lambda x: x.quantity, axis=1, result_type='expand')
    # print(a)

    # TODO: mask after cost vs cost after mask(current)
    item_count = recipe_group.apply(lambda x: x.price.isnull().sum()).reset_index(name="item_count")
    recipe_cost = recipe_group.apply(lambda x: x.price.sum()).reset_index(name="cost")

    # TODO: Using queryBuilder, make code more clean.
    return pd.merge(load_recipe(),
                    mask_by_count(item_count, recipe_cost),
                    how="inner", left_on="id", right_on="recipe_id"
                    ) \
        .drop(['recipe_id'], axis=1)
