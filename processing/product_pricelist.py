import json

def get_field_index(field_mappings, field_name):
    for i, field in enumerate(field_mappings):
        if field_name in field:
            return i
    return None  # Field name not found in field_mappings

def process_name(row, field_mappings):
    return addTranslation(row, field_mappings, 'name')

def addTranslation(row, field_mappings, field):
    indexName = get_field_index(field_mappings, field)
    row_list = list(row)  # Convert tuple to list
    name = {
        "en_US" : row_list[indexName],
        "nl_NL" : row_list[indexName]
    }
    row_list[indexName] = json.dumps(name)
    row = tuple(row_list)  # Convert list back to tuple
    return row