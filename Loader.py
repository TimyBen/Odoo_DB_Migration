import os
import json
import xml.etree.ElementTree as ET
import psycopg2
from psycopg2 import sql
import logging
from typing import Dict, List, Tuple, Optional
import re
from datetime import datetime
import traceback
import importlib
import copy

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


class DataMigration:

    def __init__(self, connection_file: str, models_xml_directory: str, mapping_directory: str):
        self.connection_file = connection_file
        self.models_xml_directory = models_xml_directory
        self.mapping_directory = mapping_directory
        self.connection_data = self.load_connection_data()

    # Loads database connection parameters from a JSON file.
    def load_connection_data(self) -> Dict[str, Dict]:

        try:
            with open(self.connection_file, 'r') as file:
                return json.load(file)
        except Exception as e:
            logging.error("Failed to load connection data: %s", e)
            raise

    # Loads the model mappings from an XML file.
    def get_model_mappings(self, model_xml_name: str) -> Optional[Dict]:

        file_path = os.path.join(self.mapping_directory, f"{model_xml_name}.xml")
        if not os.path.exists(file_path):
            logging.warning("XML file not found for model: %s", model_xml_name)
            return None
        return self.read_mapping_file(file_path)

    # Parses a mapping XML file to extract model mappings.
    def read_mapping_file(self, file_path: str) -> List[Dict]:

        try:
            tree = ET.parse(file_path)
            root = tree.getroot()
            mappings = []
            global start_id
            for mapping in root.findall('mapping'):
                old_model = mapping.find('old_model').text
                new_model = mapping.find('new_model').text
                start_id_field = mapping.find('start_id')
                if start_id_field is not None:
                    start_id = int(start_id_field.text)

                field_mappings = []
                functions = {}
                for field in mapping.find('fields'):
                    old_field = field.find('old_field').text
                    new_field = field.find('new_field').text
                    skip = field.find('skip')
                    function = field.find('function')  # Extract function node
                    field_function = function.text if function is not None else None
                    if field_function:
                        functions[new_field] = field_function

                    if not old_field or not new_field or skip is not None:
                        continue

                    datatype = field.find('datatype')  # Check for datatype tag
                    if old_field == 'id':
                        field_mappings.insert(0, (old_field, new_field))
                    elif datatype is not None:
                        field_mappings.append((old_field, new_field, datatype.text))
                    else:
                        field_mappings.append((old_field, new_field))

                defaults = {}
                default_elements = mapping.find('defaults')  # Check if defaults are defined
                if default_elements is not None:  # Ensure defaults are not None
                    for default in default_elements:
                        field_name = default.find('field').text
                        default_value = default.find('value').text
                        defaults[field_name] = default_value

                mapping = {
                    'old_model': old_model,
                    'new_model': new_model,
                    'field_mappings': field_mappings,
                    'defaults': defaults,
                    'functions': functions  # Include functions in the mapping,
                }
                mappings.append(mapping)
                return mappings
        except ET.ParseError as e:
            logging.error("Failed to parse mapping file: %s", e)
            return []

    # Main method to handle the data migration process.
    def migrate(self):

        old_db = self.connect_to_db(self.connection_data['old_db'])
        new_db = self.connect_to_db(self.connection_data['new_db'])

        model_xml_names = self.get_xml_names(os.path.join(self.models_xml_directory, 'models.xml'))
        for xml_name in model_xml_names:
            mappings = self.get_model_mappings(xml_name)
            if mappings:
                self.migrate_data(old_db, new_db, xml_name, mappings)

        old_db.close()
        new_db.close()

    def add_skip_to_mapping(self,old_model, column_name, xml_name):
        file_path = os.path.join(self.mapping_directory, f"{xml_name}.xml")

        try:
            tree = ET.parse(file_path)
            root = tree.getroot()

            for mapping in root.findall('mapping'):
                if mapping.find('old_model').text == old_model:
                    fields = mapping.find('fields')
                    for field in fields.findall('field'):
                        old_field = field.find('old_field').text
                        new_field = field.find('new_field').text
                        if old_field == column_name and new_field != '<skip/>':
                            skip_element = ET.Element('skip')
                            field.append(skip_element)

            tree.write(file_path)
            print(f"Updated XML file: {file_path}")
        except Exception as e:
            print(f"Error updating XML file: {e}")

    # Establishes a database connection with given credentials.
    def connect_to_db(self, credentials: Dict[str, str]):

        try:
            return psycopg2.connect(
                host=credentials["host"],
                dbname=credentials["db"],
                user=credentials["user"],
                password=credentials["password"]
            )
        except psycopg2.Error as e:
            logging.error("Database connection failed: %s", e)
            raise

    # Extracts XML names from the models XML file.
    def get_xml_names(self, models_xml_path: str) -> List[str]:
        model_names = []
        try:
            tree = ET.parse(models_xml_path)
            root = tree.getroot()
            for model in root.findall('model'):
                if model.find('xml_name') is not None:
                    name = model.find('xml_name').text.strip()
                else:
                    name = model.find('new_name').text.strip()
                print(name)
                model_names.append(name)
        except Exception as e:
            print(f"Error parsing models.xml: {e}")
        return model_names

    def convert_to_datetime(self,date_string):
        try:
            # Adjust the format based on the format of your datetime strings in the database
            datetime_obj = datetime.strptime(date_string, '%Y-%m-%d %H:%M:%S.%f')
            return datetime_obj
        except ValueError:
            # Handle any parsing errors here
            return None  # Or raise an exception, depending on your error handling strategy

    # Performs data migration based on provided mappings.
    def migrate_data(self, old_db_conn, new_db_conn,xml_name, mappings: List[Dict]):

        logging.info("The migrating the data is processing...")
        old_cursor = old_db_conn.cursor()
        new_cursor = new_db_conn.cursor()

        for mapping_data in mappings:
            module = False
            unresolved_skips = True
            old_model = mapping_data['old_model']
            new_model = mapping_data['new_model']
            field_mappings = mapping_data['field_mappings']
            defaults = mapping_data['defaults']
            functions = mapping_data.get('functions', {})
            old_fields = ', '.join([f[0] for f in field_mappings])
            new_fields = ', '.join([f[1] for f in field_mappings])
            placeholders = ', '.join(['%s'] * len(field_mappings))

            if functions:
                module_name = f"processing.{new_model.lower()}"
                try:
                    # Import the module dynamically
                    module = importlib.import_module(module_name)
                except ImportError:
                    # Handle the case where the module cannot be imported
                    logging.info(f"Error: process file {module_name} not found or could not be imported.")

            # Read data from old model
            skip_columns = [];
            while unresolved_skips:
                unresolved_skips = False  # Reset the flag for each iteration
                try:
                    old_fields_list = []
                    for field in field_mappings:
                        old_field = field[0]
                        if old_field not in skip_columns:  # Exclude skipped columns
                            if old_field == 'id':
                                old_fields_list.insert(0, old_field)
                            else:
                                old_fields_list.append(old_field)
                        else:
                            logging.info(f"Skipping column {old_field}")
                    old_fields = ', '.join(old_fields_list)
                    # Read data from old model
                    if start_id > 0:
                        old_cursor.execute(f'SELECT {old_fields} FROM {old_model} WHERE id >= {start_id}')
                    else:
                        old_cursor.execute(f'SELECT {old_fields} FROM {old_model}')
                    rows = old_cursor.fetchall()
                except psycopg2.errors.UndefinedColumn as e:
                    # Handle the case where a column is not found
                    column_name = re.findall(r'column "(.*?)" does not exist', str(e))
                    logging.info(f"Error finding column name: {column_name}")
                    if column_name:
                        self.add_skip_to_mapping(old_model, column_name[0], xml_name)
                        skip_columns.append(column_name[0])  # Add the skipped column to the set
                        unresolved_skips = True  # Set the flag to repeat the loop
                        old_db_conn.rollback()
                    else:
                        logging.info(f"Error parsing column name: {e}")

            # Process retrieved rows
            logging.info(f"importing {len(rows)} rows from {xml_name}")
            for row in rows:
                update_row = row
                try:
                    # Assuming the first field is the unique identifier
                    unique_id = row[0]
                    # Handle datetime fields
                    for i, field in enumerate(field_mappings):
                        if 'c_datetime' in field:
                            if isinstance(row[i], str):  # Check if it's a string
                                row[i] = self.convert_to_datetime(row[i])
                        if field[1] in functions:
                            module_name = f"processing.{new_model.lower()}"
                            if module:
                                try:
                                    function = getattr(module, functions[field[1]])
                                    update_row = function(update_row, field_mappings)
                                except AttributeError as e:
                                    logging.info(
                                        f"Error: Can not find function {functions[field[1]]} in {module_name}.")
                                except Exception as e:
                                    logging.info(traceback.format_exc())

                    check_query = f"SELECT EXISTS(SELECT 1 FROM {new_model} WHERE id=%s)"
                    new_cursor.execute(check_query, (unique_id,))
                    exists = new_cursor.fetchone()[0]
                    if exists:
                        update_fields = ', '.join([f"{f} = %s" for f in new_fields.split(', ')])
                        update_query = f"UPDATE {new_model} SET {update_fields} WHERE id = %s"
                        update_data = update_row[0:] + (unique_id,)
                        new_cursor.execute(update_query, update_data)
                    else:
                        new_field_list = new_fields.split(', ')
                        placeholders_list = placeholders

                        # Check if default fields are defined and add them if not
                        for field_name, default_value in defaults.items():
                            if field_name not in new_field_list:
                                new_field_list.append(field_name)
                                placeholders_list += ", %s"
                                update_row += (default_value,)  # Add default value to the row

                        # Join the list back into a comma-delimited string
                        new_fields_insert = ', '.join(new_field_list)
                        # Insert new record
                        insert_query = f"INSERT INTO {new_model} ({new_fields_insert}) VALUES ({placeholders_list})"
                        new_cursor.execute(insert_query, update_row)

                    new_db_conn.commit()
                except Exception as e:
                    logging.info(traceback.format_exc())
                    logging.info(f"Error processing row from {old_model} to {new_model}: {e}")
                    new_db_conn.rollback()

            self.setup_auto_increment(new_db_conn, new_model, 'id')

        old_cursor.close()
        new_cursor.close()
        logging.info("The data migration is completed!")

    # TODO : Adjust Auto Increment ID from table after loop to the highest ID +1 from the import

    def setup_auto_increment(self, conn, table_name, id_column):
        try:
            cur = conn.cursor()
            # Step 1: Create a sequence if it doesn't exist
            sequence_name = f'{table_name}_{id_column}_seq'
            cur.execute(sql.SQL("CREATE SEQUENCE IF NOT EXISTS {}").format(sql.Identifier(sequence_name)))
            # Step 2: Alter the table to set the default value of the ID column to the next value from the sequence
            cur.execute(
                sql.SQL(""" ALTER TABLE {} ALTER COLUMN {} SET DEFAULT nextval({});""")
                .format(sql.Identifier(table_name), sql.Identifier(id_column), sql.Literal(sequence_name))
            )
            # Step 3: Set the sequence's starting point based on the current maximum ID
            cur.execute(
                sql.SQL("""SELECT setval({}, COALESCE((SELECT MAX({}) FROM {}), 0) + 1, false);""")
                .format(sql.Literal(sequence_name), sql.Identifier(id_column), sql.Identifier(table_name))
            )
            # Commit the changes
            conn.commit()
            # Close communication with the PostgreSQL database server
            cur.close()
            logging.info('adjusted sequence')
        except (Exception, psycopg2.DatabaseError) as error:
            logging.info(f"Error while setting auto increment : {error}")

# main
if __name__ == "__main__":
    dm = DataMigration(
        connection_file='connection.json',
        models_xml_directory="/var/www/ontw/odoo/migration",
        mapping_directory="/var/www/ontw/odoo/migration/mappings"
    )
    dm.migrate()
