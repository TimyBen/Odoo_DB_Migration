import os
import json
import xml.etree.ElementTree as ET
import psycopg2  # PostgreSQL database adapter for Python
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logging.info('Mapping Started...')


class Loader:

    # Initialized the Enviroment
    def __init__(self):

        # Read database connection parameters from 'connection.json'
        with open('connection.json', 'r') as json_file:
            self.connection_data = json.load(json_file)

        # Database connection parameters for the old database (Odoo 10)
        self.old_db_params = self.connection_data["old_db"]

        # Database connection parameters for the new database (Odoo 16)
        self.new_db_params = self.connection_data["new_db"]

        # Define the 'mapping' directory if it doesn't exist
        if not os.path.exists("mappings"):
            os.makedirs("mappings")

    # Define a function to check if a model exists in the new database (Odoo 16)
    def _model_exists_in_db(self, model_name, db_params):
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
                logging.info(f"Error checking model ({model_name}) existence in the {host}")
                return False
        except psycopg2.Error as e:
            logging.error(f"Error checking model ({model_name}) existence in the new database: {e}")
            return False
        return True

    # Define a function to fetch field mappings for a model from the old database (Odoo 10)
    def _fetch_field_mappings(self, model_name, table_name, db_params, key, field_mappings={}):
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
            logging.error(f"Error fetching field mappings from the old database: {e}")
            return []

    # Define a function to generate XML for a model
    def _generate_model_mapping(self, old_model_name, new_model_name, table_name, field_mappings, xml_name):

        # Define the file path for the XML file in the 'mappings' directory
        if xml_name:
            file_path = os.path.join("mappings", f"{xml_name}.xml")
        else:
            file_path = os.path.join("mappings", f"{new_model_name}.xml")

        # Check if the file already exists
        if os.path.exists(file_path):
            logging.info(f"XML file already exists: {file_path}")
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
        logging.info(f"Created XML file: {file_path}")

    def _load(self):

        # Parse 'models.xml' to get model names and table names
        model_data = ET.parse('models.xml').getroot()
        old_model_names = [model.find('old_name').text for model in model_data.findall('model')]
        new_model_names = [model.find('new_name').text for model in model_data.findall('model')]
        old_table_names = [model.find('old_table').text for model in model_data.findall('model')]
        new_table_names = [model.find('new_table').text for model in model_data.findall('model')]
        xml_name = [model.find('xml_name').text for model in model_data.findall('model')]

        # Loop through model names and fetch and generate XML for each model
        for old_model_name, new_model_name, old_table_name, new_table_name, xml_name in zip(old_model_names,
                                                                                            new_model_names,
                                                                                            old_table_names,
                                                                                            new_table_names,
                                                                                            xml_name):
            # Check if the model exists in the new database (Odoo 16)
            if self._model_exists_in_db(old_model_name, self.old_db_params) and self._model_exists_in_db(new_model_name,
                                                                                                         self.new_db_params):
                field_mappings = self._fetch_field_mappings(old_model_name, old_table_name, self.old_db_params,
                                                            'old_field')
                field_mappings = self._fetch_field_mappings(new_model_name, new_table_name, self.new_db_params,
                                                            'new_field',
                                                            field_mappings)
                if not xml_name:
                    xml_name = False
                self._generate_model_mapping(old_model_name, new_model_name, new_table_name, field_mappings, xml_name)
            else:
                logging.info(
                    f"Model '{old_model_name}' does not exist in the new database (Odoo 16). Skipping XML generation.")


if __name__ == "__main__":
    loader = Loader()
    loader._load()
    logging.info('Mapping Ended!')
