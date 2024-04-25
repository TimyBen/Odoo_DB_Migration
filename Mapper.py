import os
import json
import xml.etree.ElementTree as ET
import psycopg2  # PostgreSQL database adapter for Python
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logging.info('Mapping Started...')


class Mapper:

    def _get_db_params(self):
        with open('connection.json', 'r') as json_file:
            connection_data = json.load(json_file)
            # Add a key to differentiate between old and new database parameters
            db_params = {
                "old": connection_data.get("old_db"),
                "new": connection_data.get("new_db")
            }
            return db_params

    def _create_mappings_directory(self):
        if not os.path.exists("mappings"): os.makedirs("mappings")

    # Initialized the Enviroment
    def __init__(self):
        self.db_params = self._get_db_params()
        self._create_mappings_directory()

    def _connect_to_database(self, version):
        """
        Establishes a connection to the specified version of the database.
        Args: version (str): The version of the database to connect to ('old' or 'new').
        Returns: psycopg2.connection: The connection object.
        """
        try:
            conn = psycopg2.connect(
                host=self.db_params[version]["host"],
                dbname=self.db_params[version]["db"],
                port=self.db_params[version]["port"],
                user=self.db_params[version]["user"],
                password=self.db_params[version]["password"]
            )
            return conn
        except psycopg2.Error as e:
            logging.error(f"Error connecting to the {version} database: {e}")
            return None

    def _model_exists_in_db(self, model_name, version):
        try:
            conn = self._connect_to_database(version)
            cursor = conn.cursor()
            sql_query = f"SELECT COUNT(*) FROM ir_model WHERE model = '{model_name}';"
            cursor.execute(sql_query)
            model_count = cursor.fetchone()[0]
            cursor.close()
            conn.close()
            if not model_count > 0:
                logging.info(f"Model '{model_name}' does not exist in the {version} database.")
                return False
        except psycopg2.Error as e:
            logging.error(f"Error checking model ({model_name}) existence in the {version} database: {e}")
            return False
        return True

    def _fetch_query(self, model_name, table_name):
        return f"""
            SELECT imf.name, imf.ttype, imf.relation_field, imf.relation AS field
                FROM ir_model im
                LEFT JOIN ir_model_fields imf ON imf.model_id = im.id
                JOIN (
                    SELECT column_name 
                    FROM information_schema.columns 
                    WHERE table_schema = 'public' 
                    AND table_name = '{table_name}'
                ) AS columns_filter
                ON columns_filter.column_name = imf.name
                WHERE im.model = '{model_name}' ;
        """

    def _fetch_results(self, cursor, key, field_mappings):
        """
        Fetch results from the cursor and update field mappings.
        """
        # print("Fetch Result", field_mappings)
        for field in cursor.fetchall():
            field_info = {
                'name': field[0],
                'ttype': field[1],
                'relation_field': field[2],
                'relation': field[3]
            }
            if field_info['name'] not in field_mappings:
                field_mappings[field_info['name']] = {}
            field_mappings[field_info['name']][f'{key}_field'] = field_info

    # Define a function to fetch field mappings for a model from the old database (Odoo 10)
    def _fetch_field_mappings(self, model_name, table_name, key, field_mappings={}):
        try:
            conn = self._connect_to_database(key)
            cursor = conn.cursor()
            sql_query = self._fetch_query(model_name, table_name)
            cursor.execute(sql_query)
            self._fetch_results(cursor, key, field_mappings)

            cursor.close()
            conn.close()
            return field_mappings
        except psycopg2.Error as e:
            logging.error(f"Error fetching field mappings from the old database: {e}")
            return []

    def _generate_field_element(self, field_name, field_info):
        field = ET.Element("field")
        # Add old field information if available
        if "old_field" in field_info:
            old_name = ET.SubElement(field, "old_field")
            old_name.text = field_info["old_field"]["name"]
            old_ttype = ET.SubElement(field, "old_field_type")
            old_ttype.text = field_info["old_field"]["ttype"]
            old_relation_field = ET.SubElement(field, "old_relation_field")
            old_relation_field.text = field_info["old_field"]["relation_field"]
            old_relation = ET.SubElement(field, "old_relation")
            old_relation.text = field_info["old_field"]["relation"]

        # Add new field information if available
        if "new_field" in field_info:
            new_name = ET.SubElement(field, "new_field")
            new_name.text = field_info["new_field"]["name"]
            new_ttype = ET.SubElement(field, "new_field_type")
            new_ttype.text = field_info["new_field"]["ttype"]
            new_relation_field = ET.SubElement(field, "new_relation_field")
            new_relation_field.text = field_info["new_field"]["relation_field"]
            new_relation = ET.SubElement(field, "new_relation")
            new_relation.text = field_info["new_field"]["relation"]

        return field

    def _generate_model_mapping(self, old_model_name, new_model_name, table_name, field_mappings, xml_name):
        """
        Generate XML mapping for old and new model fields.
        """
        # Define the file path for the XML file in the 'mappings' directory
        if xml_name:
            file_path = os.path.join("mappings", f"{xml_name}.xml")
        else:
            file_path = os.path.join("mappings", f"{new_model_name}.xml")

        # Create the root element for the XML
        mappings = ET.Element("mappings")
        # Create a mapping entry for the model
        mapping = ET.SubElement(mappings, "mapping")
        old_model = ET.SubElement(mapping, "old_model")
        old_model.text = old_model_name
        new_model = ET.SubElement(mapping, "new_model")
        new_model.text = new_model_name

        # Add start_id element
        start_id = ET.SubElement(mapping, "start_id")
        start_id.text = "1"

        # Generate and add defaults element
        defaults = self._generate_defaults_element(field_mappings)
        mapping.append(defaults)

        # Create a 'fields' element
        fields = ET.SubElement(mapping, "fields")

        # Handle the "id" field separately to ensure it appears first
        id_field_info = field_mappings.get("id")
        if id_field_info:
            id_field = self._generate_field_element("id", id_field_info)
            fields.append(id_field)
            del field_mappings["id"]  # Remove the "id" field from the field mappings

        # Loop through remaining fields in the old model
        for field_name, field_info in field_mappings.items():
            field = self._generate_field_element(field_name, field_info)
            fields.append(field)

        # Create the XML tree
        tree = ET.ElementTree(mappings)
        # Save the XML to the file path
        tree.write(file_path)
        # Print the name of the created XML file
        logging.info(f"Created XML file: {file_path}")

    def _generate_defaults_element(self, field_mappings):
        """
        Generate the <defaults> element with default values for each new field.
        """
        defaults = ET.Element("defaults")
        for field_name, field_info in field_mappings.items():
            if "new_field" in field_info:
                default = ET.SubElement(defaults, "default")
                field_name_element = ET.SubElement(default, "field")
                field_name_element.text = field_info["new_field"]["name"]
                value_element = ET.SubElement(default, "value")
                value_element.text = "_______________"  # Set default value as empty string
        return defaults

    def _parse_model_data(self):
        """
        Parses the 'models.xml' file to retrieve model data.
        Returns:
            list: A list of tuples containing the old model name, new model name,
            old table name, new table name, and XML name for each model.
        """
        model_data = ET.parse('models.xml').getroot()
        parsed_data = []
        for model in model_data.findall('model'):
            parsed_data.append((
                model.find('old_name').text,
                model.find('new_name').text,
                model.find('old_table').text,
                model.find('new_table').text,
                model.find('xml_name').text
            ))
        return parsed_data

    def _map(self):
        """
        Maps models from the old database to the new database.
        This method iterates through the parsed model data, checks if each model exists
        in the new database, and generates XML mappings if applicable.
        """
        model_data = self._parse_model_data()  # 0_old_model, 1_new_model, 2_old_table, 3_new_table, 4_xml_name
        for model_info in model_data:
            if self._model_exists_in_db(model_info[0], "old"):  # Checking old model existence
                field_mappings = self._fetch_field_mappings(model_info[0], model_info[2], 'old')
                if self._model_exists_in_db(model_info[1], "new"):  # Checking new model existence
                    field_mappings = self._fetch_field_mappings(model_info[1], model_info[3], 'new', field_mappings)
                    if not model_info[4]:
                        xml_name = False
                    else:
                        xml_name = model_info[4]
                    self._generate_model_mapping(model_info[0], model_info[1], model_info[3], field_mappings, xml_name)
                else:
                    logging.info(
                        f"Model '{model_info[1]}' does not exist in any of the databases. Skipping XML generation.")
            else:
                logging.info(f"Model '{model_info[0]}' does not exist in Skipping XML generation.")


if __name__ == "__main__":
    mapper = Mapper()
    mapper._map()
    logging.info('Mapping Ended!')
