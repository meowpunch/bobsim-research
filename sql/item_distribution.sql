SELECT
    item.id,
    item.name,
    item_frequency,
    average,
    delta,
    distr_type
FROM item
INNER JOIN item_frequency ON item.id = item_frequency.item_id
INNER JOIN price ON item.id = price.item_id
;