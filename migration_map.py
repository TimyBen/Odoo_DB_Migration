import os
import json
import xml.etree.ElementTree as ET
import psycopg2  # PostgreSQL database adapter for Python

# Read database connection parameters from 'connection.json'
with open('connection.json', 'r') as json_file:
    connection_data = json.load(json_file)

# Database connection parameters for the old database (Odoo 10)
old_db_params = connection_data["old_db"]

# Database connection parameters for the new database (Odoo 16)
new_db_params = connection_data["new_db"]

# Define the 'mapping' directory if it doesn't exist
if not os.path.exists("mappings"):
    os.makedirs("mappings")

# Define a function to check if a model exists in the new database (Odoo 16)
def model_exists_in_db(model_name, db_params):
    try:
        # Connect to the new database (Odoo 16)
        host = db_params["host"];
        conn = psycopg2.connect(
            host=host,
            dbname=db_params["db"],
            user=db_params["user"],
            password=db_params["password"]
        )

        # Create a cursor
        cursor = conn.cursor()

        # Execute a SQL query to check if the model exists in 'ir_model' table
        sql_query = f"SELECT COUNT(*) FROM ir_model WHERE model = '{model_name}';"
        cursor.execute(sql_query)

        # Fetch the count result
        model_count = cursor.fetchone()[0]

        # Close the cursor and connection
        cursor.close()
        conn.close()

        if not model_count > 0:
            print(f"Error checking model ({model_name}) existence in the {host}")
            return False
    except psycopg2.Error as e:
        print(f"Error checking model ({model_name}) existence in the new database: {e}")
        return False
    return True

# Define a function to fetch field mappings for a model from the old database (Odoo 10)
def fetch_field_mappings(model_name, table_name, db_params, key, field_mappings={}):
    try:
        # Connect to the old database (Odoo 10)
        conn = psycopg2.connect(
            host=db_params["host"],
            dbname=db_params["db"],
            user=db_params["user"],
            password=db_params["password"]
        )

        # Create a cursor
        cursor = conn.cursor()

        # Execute a SQL query to fetch field mappings for the model from 'ir_model_fields' table
        sql_query = f"""
            SELECT imf.name AS field
            FROM ir_model im
            LEFT JOIN ir_model_fields imf ON imf.model_id = im.id
            WHERE im.model = '{model_name}' OR im.model = '{table_name}';
        """
        cursor.execute(sql_query)
        # Fetch the results
        for field in cursor.fetchall():
            if field not in field_mappings:
                field_mappings[field] = {}
            field_mappings[field][key] = field[0]

        # Close the cursor and connection
        cursor.close()
        conn.close()

        return field_mappings
    except psycopg2.Error as e:
        print(f"Error fetching field mappings from the old database: {e}")
        return []

# # Define a function to fetch field names for a model from the new database (Odoo 16)
# def fetch_field_mappings_from_new_db(model_name, table_name):
#     try:
#         # Connect to the new database (Odoo 16)
#         conn = psycopg2.connect(
#             host=new_db_params["host"],
#             dbname=new_db_params["db"],
#             user=new_db_params["user"],
#             password=new_db_params["password"]
#         )
#
#         # Create a cursor
#         cursor = conn.cursor()
#
#         # Execute a SQL query to fetch field names for the model from 'ir_model_fields' table
#         sql_query = f"""
#             SELECT name
#             FROM ir_model_fields
#             WHERE model_id = (SELECT id FROM ir_model WHERE model = '{model_name}');
#         """
#         cursor.execute(sql_query)
#
#         # Fetch the results
#         field_names = [name for (name,) in cursor.fetchall()]
#
#         # Close the cursor and connection
#         cursor.close()
#         conn.close()
#
#         return field_names
#     except psycopg2.Error as e:
#         print(f"Error fetching field names from the new database: {e}")
#         return []

# Define a function to generate XML for a model
def generate_model_mapping(old_model_name,new_model_name, table_name, field_mappings, xml_name):

    # Define the file path for the XML file in the 'mappings' directory
    if xml_name:
        file_path = os.path.join("mappings", f"{xml_name}.xml")
    else:
        file_path = os.path.join("mappings", f"{new_model_name}.xml")

    # Check if the file already exists
    if os.path.exists(file_path):
        print(f"XML file already exists: {file_path}")
        return  # File exists, no need to recreate it

    # Create the root element for the XML
    mappings = ET.Element("mappings")

    # Create a mapping entry for the model
    mapping = ET.SubElement(mappings, "mapping")
    old_model = ET.SubElement(mapping, "old_model")
    old_model.text = old_model_name
    new_model = ET.SubElement(mapping, "new_model")
    new_model.text = new_model_name

    # Create a 'fields' element
    fields = ET.SubElement(mapping, "fields")

    # Loop through field mappings and create 'field' elements
    for field_mapping in field_mappings.values():
        field = ET.SubElement(fields, "field")
        old_field = ET.SubElement(field, "old_field")
        new_field = ET.SubElement(field, "new_field")

        if "old_field" in field_mapping:
            old_field.text = field_mapping['old_field']
        else:
            old_field.text = ""

        if "new_field" in field_mapping:
            new_field.text = field_mapping['new_field']
        else:
            new_field.text = ""

    # Create the XML tree
    tree = ET.ElementTree(mappings)

    # Define the file path for the XML file in the 'mapping' directory
    if xml_name:
        file_path = os.path.join("mappings", f"{xml_name}.xml")
    else:
        file_path = os.path.join("mappings", f"{new_model_name}.xml")

    # Save the XML to the file path
    tree.write(file_path)

    # Print the name of the created XML file
    print(f"Created XML file: {file_path}")

# Parse 'models.xml' to get model names and table names
model_data = ET.parse('models.xml').getroot()
old_model_names = [model.find('old_name').text for model in model_data.findall('model')]
new_model_names = [model.find('new_name').text for model in model_data.findall('model')]
old_table_names = [model.find('old_table').text for model in model_data.findall('model')]
new_table_names = [model.find('new_table').text for model in model_data.findall('model')]
xml_name = [model.find('xml_name').text for model in model_data.findall('model')]

# Loop through model names and fetch and generate XML for each model
for old_model_name, new_model_name, old_table_name, new_table_name, xml_name in zip(old_model_names, new_model_names,old_table_names,new_table_names, xml_name):
    # Check if the model exists in the new database (Odoo 16)
    if model_exists_in_db(old_model_name, old_db_params) and model_exists_in_db(new_model_name,new_db_params):
        field_mappings = fetch_field_mappings(old_model_name, old_table_name, old_db_params, 'old_field')
        field_mappings = fetch_field_mappings(new_model_name, new_table_name, new_db_params, 'new_field', field_mappings)
        if not xml_name:
            xml_name = False
        generate_model_mapping(old_model_name,new_model_name, new_table_name, field_mappings, xml_name)
    else:
        print(f"Model '{old_model_name}' does not exist in the new database (Odoo 16). Skipping XML generation.")
