import json

def get_field_index(field_mappings, field_name):
    for i, field in enumerate(field_mappings):
        if field_name in field:
            return i
    return None  # Field name not found in field_mappings



def process_price_unit(row, field_mappings):
    indexPriceUnit = get_field_index(field_mappings, 'price_unit')
    row_list = list(row)
    if indexPriceUnit:
        priceUnit = row_list[indexPriceUnit]
        if priceUnit is None :
            row_list[indexPriceUnit] = 0
    row = tuple(row_list)
    return row
