'''
Module to parse a set of test results in JSON, extract relevant metrics 
to a PostgreSQL database and visualise through Grafana
'''

#Import all necessary libraries
import os
import json
import psycopg2
from psycopg2 import sql

def connect_to_db(params):
    '''Function to connect to database'''
    connection = psycopg2.connect(**params)
    cursor = connection.cursor()
    print("Connection to PostgreSQL established successfully.")
    return connection, cursor

def close_db(connection, cursor):
    '''Function to close database connection'''
    if cursor:
        cursor.close()
    if connection:
        connection.close()
    print("PostgreSQL connection closed.")

def create_log_file_table(cursor):
    '''Function to create a Log Table with record of all parsed .json files'''
    create_log_file_query = '''
        CREATE TABLE IF NOT EXISTS parsed_files (
            file_name varchar(300)  
        )'''
    cursor.execute(create_log_file_query)

def check_file_already_parsed(cursor, filename):
    '''Function to check if a particular JSON file is already recorded 
    into the Log Table and thus the corresponding test table'''
    cursor.execute('''SELECT EXISTS (SELECT 1 FROM parsed_files
                    WHERE file_name = %s)''', (filename,))
    return cursor.fetchone()[0]

def log_parsed_file(cursor, filename):
    '''Function to insert the unparsed JSON file into the Log Table on parsing'''
    cursor.execute('INSERT INTO parsed_files (file_name) VALUES (%s)', (filename,))

def parse_files(connection, cursor, directory):
    '''Function to parse through all JSON files present in the directory and extracting its data'''
    parsed_data = []
    for filename in os.listdir(directory):
        if check_file_already_parsed(cursor, filename):
            continue
        file_path = os.path.join(directory, filename)
        with open(file_path, 'r', encoding='utf-8') as file:
            data = json.load(file)
            parsed_data.append(data)
        log_parsed_file(cursor, filename)
        connection.commit()
    return parsed_data

def create_testresult_table(cursor, table_name):
    '''Fucntion to create table for each Test'''
    create_table_query = sql.SQL('''
        CREATE TABLE IF NOT EXISTS {} (
            key varchar(200),
            board_type varchar(200),
            boot_type varchar(200),
            release varchar(200),
            config varchar(200),
            last_modified TIMESTAMP,
            processor varchar(200),
            memory varchar(200),
            disk varchar(200),
            graphics varchar(200),
            network varchar(200),
            os varchar(200),
            kernel varchar(200),
            app_title varchar(200),
            app_version varchar(200),
            test_description varchar(200),
            unit varchar(200),
            value float
        )
    ''').format(sql.Identifier(table_name))
    cursor.execute(create_table_query)

def insert_into_testresult_table(cursor, table_name, values):
    '''Function to insert the required parameters into the table of the corresponding test'''
    insert_table_query = sql.SQL('''
        INSERT INTO {} (
            key, board_type, boot_type, release, config,
            last_modified, processor, memory, disk, graphics,
            network, os, kernel, app_title, app_version,
            test_description, unit, value
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ''').format(sql.Identifier(table_name))
    cursor.execute(insert_table_query, values)

def process_parsed_data(cursor, parsed_data):
    '''Function that creates and inserts records extracted from the JSON through 
    nested keys and slices into the corresponding test tables using above helper functions.'''
    for file in parsed_data:
        l = file["title"].split("-")
        table_name_str = l[0]

        create_testresult_table(cursor, table_name_str)

        for result in file["results"].values():
            if "value" not in result["results"].get(file["title"][:-13], {}):
                continue

            if table_name_str != 'perfbench':
                app_version = result["app_version"]
            else:
                app_version = result["identifier"].split("-")[-1]

            values = (
                file["title"],
                l[1] + "-" + l[2],
                l[3],
                l[4],
                l[5],
                file["last_modified"],
                file["systems"][file["title"][:-13]]["hardware"]["Processor"],
                file["systems"][file["title"][:-13]]["hardware"]["Memory"],
                file["systems"][file["title"][:-13]]["hardware"]["Disk"],
                file["systems"][file["title"][:-13]]["hardware"]["Graphics"],
                file["systems"][file["title"][:-13]]["hardware"]["Network"],
                file["systems"][file["title"][:-13]]["software"]["OS"],
                file["systems"][file["title"][:-13]]["software"]["Kernel"],
                result["title"],
                app_version,
                result["description"],
                result["scale"],
                result["results"][file["title"][:-13]]["value"]
            )
            insert_into_testresult_table(cursor, table_name_str, values)

def main():
    '''The main function'''
    db_params = {
    'dbname': 'Test',
    'user': 'postgres',
    'password': 'b2etr2ot',
    'host': 'localhost',
    'port': '5432'
     }

    directory = 'Downloads/apache_results'

    connection, cursor = connect_to_db(db_params)
    if not connection or not cursor:
        return

    try:
        create_log_file_table(cursor)
        connection.commit()

        parsed_data = []
        parsed_data=  parse_files(connection, cursor, directory)

        process_parsed_data(cursor, parsed_data)
        connection.commit()

    except ConnectionError as error:
        print(f"Error in Connecting to Database: {error}")

    finally:
        close_db(connection, cursor)

if __name__ == "__main__":
    main()
