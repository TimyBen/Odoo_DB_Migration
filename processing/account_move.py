import json

def get_field_index(field_mappings, field_name):
    for i, field in enumerate(field_mappings):
        if field_name in field:
            return i
    return None  # Field name not found in field_mappings


def process_date(row, field_mappings):
    indexDate = get_field_index(field_mappings, 'date')
    row_list = list(row)
    dateValue = row_list[indexDate]
    if not dateValue or dateValue == None:
        row_list[indexDate] = '1999-01-01'

    row = tuple(row_list)
    return row