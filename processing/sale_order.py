import json

def get_field_index(field_mappings, field_name):
    for i, field in enumerate(field_mappings):
        if field_name in field:
            return i
    return None  # Field name not found in field_mappings

def process_pricelist_id(row, field_mappings):
    indexPricelist = get_field_index(field_mappings, 'pricelist_id')
    row_list = list(row)
    pricelistValue = row_list[indexPricelist]
    if not pricelistValue or pricelistValue == None:
        row_list[indexPricelist] = 2
    row = tuple(row_list)
    return row