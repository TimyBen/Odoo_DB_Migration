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


class DataTypeHandler:
    @staticmethod
    def adapt_datetime_to_datetime(date_string):
        try:
            # Adjust the format based on the format of your datetime strings in the database
            datetime_obj = datetime.strptime(date_string, '%Y-%m-%d %H:%M:%S.%f')
            return datetime_obj
        except ValueError:
            # Handle any parsing errors here
            return None  # Or raise an exception, depending on your error handling strategy

    @staticmethod
    def adapt_boolean_to_char(value):
        return str(value)

    @staticmethod
    def adapt_boolean_to_text(value):
        return 'True' if value else 'False'

    @staticmethod
    def adapt_integer_to_integer(value):
        return int(value)

    @staticmethod
    def adapt_float_to_integer(value):
        return int(value)

    @staticmethod
    def adapt_selection_to_char(value):
        return (value)


class DataMigration:

    def __init__(self, connection_file: str, models_xml_directory: str, mapping_directory: str):
        self.connection_file = connection_file
        self.models_xml_directory = models_xml_directory
        self.mapping_directory = mapping_directory
        self.connection_data = self.load_connection_data()
        self.data_type_handler = DataTypeHandler()

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
                    field_mapping = {}
                    old_field_element = field.find('old_field')
                    new_field_element = field.find('new_field')
                    skip = field.find('skip')
                    function = field.find('function')  # Extract function node
                    field_function = function.text if function is not None else None
                    if field_function:
                        functions[new_field] = field_function

                    if old_field_element is None or new_field_element is None or skip is not None:
                        continue

                    old_field = old_field_element.text
                    new_field = new_field_element.text

                    field_mapping['field_name_old'] = old_field
                    field_mapping['field_name_new'] = new_field

                    # Check if old_field_type exists before accessing its text attribute
                    old_field_type_element = field.find('old_field_type')
                    if old_field_type_element is not None:
                        field_mapping['field_type_old'] = old_field_type_element.text

                        new_field_type_element = field.find('new_field_type')
                        if new_field_type_element is not None:
                            field_mapping['field_type_new'] = new_field_type_element.text

                    field_mappings.append(field_mapping)
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

    def add_skip_to_mapping(self, xml_name):
        file_path = os.path.join(self.mapping_directory, f"{xml_name}.xml")
        try:
            tree = ET.parse(file_path)
            root = tree.getroot()
            for mapping in root.findall('mapping'):
                fields = mapping.find('fields')
                for field in fields.findall('field'):
                    # old_field = field.find('old_field')
                    new_field = field.find('new_field')
                    # if old_field is None or new_field is None:
                    # if old_field is None:
                    if new_field is None:
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
                port=credentials["port"],
                dbname=credentials["db"],
                user=credentials["user"],
                password=credentials["password"]
            )
        except psycopg2.Error as e:
            logging.error("Database connection failed: %s", e)
            raise

    # Extracts XML names from the models XML file.
    def get_xml_names(self, models_xml_path: str) -> List[str]:

        try:
            tree = ET.parse(models_xml_path)
            root = tree.getroot()
            return [model.find('xml_name').text.strip() for model in root.findall('model') if
                    model.find('xml_name') is not None]
        except Exception as e:
            logging.error("Failed to parse models.xml: %s", e)
            return []

    def model_to_table(self, text):
        return text.replace(".", "_")

    # Performs data migration based on provided mappings.
    def get_old_fields_list(self, field_mappings, skip_columns):
        """
        Gets a list of old fields to be selected from the old database.
        Args:
            field_mappings: List of field mappings.
            skip_columns: List of columns to be skipped.
        Returns:
            List of old fields.
        """
        old_fields_list = []
        for field_mapping in field_mappings:
            old_field_name = field_mapping['field_name_old']
            if old_field_name not in skip_columns:
                old_fields_list.append(old_field_name)
            else:
                logging.info(f"Skipping column {old_field_name}")
        return old_fields_list

    def process_mapping_data(self, mapping_data, old_cursor, new_cursor, xml_name, old_db_conn, new_db_conn):
        """
        Processes a single mapping data.
        Args:
            mapping_data: Mapping data containing information about models, field mappings, etc.
            old_cursor: Cursor for the old database.
            new_cursor: Cursor for the new database.
            xml_name: Name of the XML file.
            old_db_conn: Connection to the old database.
            new_db_conn: Connection to the new database.
        """
        self.add_skip_to_mapping(xml_name)
        module = None
        unresolved_skips = True
        old_table = self.model_to_table(mapping_data['old_model'])
        new_table = self.model_to_table(mapping_data['new_model'])
        field_mappings = mapping_data['field_mappings']
        defaults = mapping_data['defaults']
        functions = mapping_data.get('functions', {})
        if functions:
            module_name = f"processing.{new_table.lower()}"
            try:
                module = importlib.import_module(module_name)
            except ImportError:
                logging.info(f"Error: process file {module_name} not found or could not be imported.")
        skip_columns = []
        while unresolved_skips:
            unresolved_skips = False
            try:
                old_fields_list = self.get_old_fields_list(field_mappings, skip_columns)
                old_fields = ', '.join(old_fields_list)
                old_db_conn.rollback()  # Rollback previous transaction
                old_cursor.execute(f'SELECT {old_fields} FROM {old_table}')
                rows = old_cursor.fetchall()
            except psycopg2.errors.UndefinedColumn as e:
                column_name = re.findall(r'column "(.*?)" does not exist', str(e))
                logging.info(f"Error finding column name: {column_name}")
                if column_name:
                    skip_columns.append(column_name[0])
                    unresolved_skips = True
                else:
                    logging.info(f"Error parsing column name: {e}")
        self.process_rows(rows, new_cursor, new_table, field_mappings, defaults, functions, new_db_conn)

    def process_rows(self, rows, new_cursor, new_table, field_mappings, defaults, functions, new_db_conn):
        for row in rows:
            update_row = row
            try:
                # Convert row tuple to list to make it mutable
                update_row = list(row)
                unique_id = update_row[0]  # Assuming the unique ID is at index 0

                # Create a new list starting from index 1
                data_row_trimmed = [update_row[0]] + update_row[1:]
                for field in field_mappings:  # Ensure field is a dictionary
                    old_field_type = field.get('field_type_old')
                    new_field_type = field.get('field_type_new')
                    field_index = field_mappings.index(field)
                    if old_field_type and new_field_type and old_field_type != new_field_type:
                        field_value = data_row_trimmed[field_index]
                        if field_value is not None:
                            data_type = 'adapt_' + old_field_type.lower() + '_to_' + new_field_type.lower()
                            handler_func = getattr(DataTypeHandler, data_type, None)
                            if handler_func:
                                # Update the value in the list
                                data_row_trimmed[field_index] = handler_func(field_value)
                            else:
                                print(f"No handler found for data type: {data_type}")

                # Convert the updated list back to tuple
                data_row_trimmed = tuple(data_row_trimmed)
                exists = self.check_existence_in_new_table(new_cursor, unique_id, new_table)
                if exists:
                    self.update_existing_record(new_cursor, data_row_trimmed, field_mappings, new_table, unique_id)
                else:
                    self.insert_new_record(new_cursor, data_row_trimmed, field_mappings, defaults, new_table)
                # self.setup_auto_increment(new_db_conn, new_table, 'id')
            except Exception as e:
                logging.info(traceback.format_exc())
                logging.info(f"Error processing row to {new_table}: {e}")
                new_db_conn.rollback()

    def handle_data_type(self, value, data_type):
        handler_func = getattr(self.data_type_handler, data_type)
        return handler_func(value)

    def check_existence_in_new_table(self, new_cursor, unique_id, new_table):
        """
        Checks if a record with the given unique ID exists in the new table.

        Args:
            new_cursor: Cursor for the new database.
            unique_id: Unique identifier.
            new_table: Name of the new table.

        Returns:
            True if the record exists, False otherwise.
        """
        check_query = f"SELECT EXISTS(SELECT 1 FROM {new_table} WHERE id=%s)"
        new_cursor.execute(check_query, (unique_id,))
        exists = new_cursor.fetchone()[0]
        return exists

    def update_existing_record(self, new_cursor, update_row, field_mappings, new_table, unique_id):
        """
        Updates an existing record in the new table.

        Args:
            new_cursor: Cursor for the new database.
            update_row: Tuple of data to update.
            field_mappings: List of field mappings.
            new_table: Name of the new table.
            unique_id: Unique identifier of the record.
        """
        update_fields = ', '.join([f"{field['field_name_new']} = %s" for field in field_mappings])
        update_query = f"UPDATE {new_table} SET {update_fields} WHERE id = %s"
        update_data = list(update_row) + [unique_id]
        new_cursor.execute(update_query, update_data)
        new_cursor.connection.commit()

    def insert_new_record(self, new_cursor, update_row, field_mappings, defaults, new_table):
        """
        Inserts a new record into the new table.
        Args:
            new_cursor: Cursor for the new database.
            update_row: Data to insert.
            field_mappings: List of field mappings.
            defaults: Default values for fields.
            new_table: Name of the new table.
        """
        # Create a dictionary from field_mappings and update_row
        update_row_dict = {field['field_name_new']: value for field, value in zip(field_mappings, update_row)}
        # Update the update_row_dict with default values, overwriting existing values
        for default_field, default_value in defaults.items():
            update_row_dict[default_field] = default_value
        # Construct columns and placeholders for the INSERT query
        columns = ', '.join(update_row_dict.keys())
        placeholders = ', '.join(['%s'] * len(update_row_dict))
        insert_query = f"INSERT INTO {new_table} ({columns}) VALUES ({placeholders})"
        new_cursor.execute(insert_query, tuple(update_row_dict.values()))
        new_cursor.connection.commit()

    def migrate_data(self, old_db_conn, new_db_conn, xml_name, mappings: List[Dict]):
        """
        Migrates data from old database to new database based on provided mappings.
        Args:
            old_db_conn: Connection to the old database.
            new_db_conn: Connection to the new database.
            xml_name: Name of the XML file.
            mappings: List of mappings containing information about models, field mappings, etc.
        """
        logging.info("The data migration is processing...")
        old_cursor = old_db_conn.cursor()
        new_cursor = new_db_conn.cursor()
        max_id_per_table = {}  # Store the maximum ID per table
        for mapping_data in mappings:
            self.process_mapping_data(mapping_data, old_cursor, new_cursor, xml_name, old_db_conn, new_db_conn)
            # Retrieve the maximum ID for the table after each mapping
            max_id = self.get_max_id(new_cursor, self.model_to_table(mapping_data['new_model']))
            max_id_per_table[self.model_to_table(mapping_data['new_model'])] = max_id
        # After migrating all tables, setup auto-increment for each table
        for table, max_id in max_id_per_table.items():
            self.setup_auto_increment(new_db_conn, table, max_id)
        old_cursor.close()
        new_cursor.close()
        logging.info("The data migration is completed!")

    def get_max_id(self, cursor, table_name):
        query = f"SELECT MAX(id) FROM {table_name};"
        cursor.execute(query)
        max_id = cursor.fetchone()[0]
        return max_id

    def setup_auto_increment(self, conn, table_name, max_id=None):
        """
        Set up auto-increment for the specified table and column.
        Args:
            conn (psycopg2.extensions.connection): Database connection.
            table_name (str): Name of the table.
            id_column (str): Name of the ID column.
            max_id (int, optional): The maximum ID of the imported records. Defaults to None.
        """
        try:
            # Create a cursor using a context manager
            with conn.cursor() as cur:
                # Create sequence if it doesn't exist
                sequence_name = f'{table_name}_{"id"}_seq'
                create_sequence_query = sql.SQL("CREATE SEQUENCE IF NOT EXISTS {}").format(
                    sql.Identifier(sequence_name))
                cur.execute(create_sequence_query)
                # Alter table to set default value of ID column to next value from sequence
                alter_table_query = sql.SQL("""ALTER TABLE {} ALTER COLUMN {} SET DEFAULT nextval('{}');""").format(
                    sql.Identifier(table_name),
                    sql.Identifier("id"),
                    sql.Identifier(sequence_name)
                )
                cur.execute(alter_table_query)
                # Commit changes
                conn.commit()
                # Set sequence's starting point based on the current maximum ID
                if max_id is not None:
                    self.set_sequence(conn, cur, sequence_name, max_id)  # type: ignore

        except (Exception, psycopg2.DatabaseError) as error:
            # Log error with more details
            logging.error("Error setting up auto-increment for table %s column %s: %s", table_name, "id", error)
            # Rollback transaction
            conn.rollback()

    # set sequence autoincrement
    def set_sequence(self, conn, cur, sequence_name, max_id):
        """
        Set sequence's starting point based on the current maximum ID.
        Args:
            conn (psycopg2.extensions.connection): Database connection.
            cur (psycopg2.extensions.cursor): Database cursor.
            sequence_name (str): Name of the sequence.
            max_id (int): The maximum ID.
        """
        if max_id is not None:
            set_sequence_value_query = sql.SQL("ALTER SEQUENCE {} RESTART WITH {};").format(
                sql.Identifier(sequence_name), sql.Literal(max_id + 1))
            cur.execute(set_sequence_value_query)
            # Commit changes
            conn.commit()


# main
if __name__ == "__main__":
    # Get the current working directory
    current_directory = os.path.dirname(os.path.abspath(__file__))
    # Construct paths relative to the current directory
    dm = DataMigration(
        connection_file='connection.json',
        models_xml_directory=os.path.join(current_directory),
        mapping_directory=os.path.join(current_directory, "mappings")
    )
    dm.migrate()
