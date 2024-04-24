def get_field_index(field_mappings, field_name):
    for i, field in enumerate(field_mappings):
        if field_name in field:
            return i
    return None  # Field name not found in field_mappings


def process_firstname(row, field_mappings):
    indexCompany = get_field_index(field_mappings, 'is_company')
    if indexCompany:
        indexFirstname = get_field_index(field_mappings, 'first_name')
        indexName = get_field_index(field_mappings, 'name')
        is_company = row[indexCompany]

        if is_company and indexFirstname:
            row_list = list(row)  # Convert tuple to list
            row_list[indexFirstname] = row[indexName]
            row = tuple(row_list)  # Convert list back to tuple
    return row

def process_street(row, field_mappings):
    indexStreet = get_field_index(field_mappings, 'street')
    if indexStreet:
        street = row[indexStreet]
    return row

