import pandas as pd
import yaml
import aiohttp
import asyncio
import duckdb

from api import ApiClient
from gcs import has_reached_threshold, export_duck_db_to_gcs


def create_duck_db_table_creation_string_from_schema(schema, table_name, exclusions):
    type_mapping = {
    'int64': 'BIGINT',
    'string': 'VARCHAR',
    'float64': 'DOUBLE',
    'boolean': 'BOOLEAN'}

    column_definitions = []

    for column_name, column_type in schema["columns"].items():
        if column_name not in exclusions:
            duckdb_type = type_mapping[column_type]
            column_definitions.append(f"{column_name} {duckdb_type}")

    columns_sql = ", ".join(column_definitions)
    return f"CREATE TABLE {table_name} ({columns_sql})"


def create_duck_db_table_insertion_string_from_schema(schema, exclusions):

    duckdb_columns = []
    for column_name in schema["columns"].keys():
        if column_name not in exclusions:
            duckdb_columns.append(column_name)
    
    columns_sql = ", ".join(duckdb_columns)
    return columns_sql
        


    

async def main(): 
    with open("config.YAML", "r") as config:
        config = yaml.safe_load(config)

        try:
            base_url = config["api_config"]["base_url"]
            number_of_users = config["api_config"]["number_of_users"]
            number_of_calls = config["api_config"]["number_of_calls"]
            batch_size = config["api_config"]["batch_size"]

            schema = config["schema"]
            table_name = config["target_configs"]["duckdb"]["table_name"]
            
            all_columns = list(config["schema"]["columns"].keys())
            duck_db_exclusions = list(config["target_configs"]["duckdb"]["exclude_columns"])

            table_creation_string = create_duck_db_table_creation_string_from_schema(schema, table_name, duck_db_exclusions)
            table_insertion_string = create_duck_db_table_insertion_string_from_schema(schema, duck_db_exclusions)

            gcs_file_rows_threshold = config["gcs"]["row_size"]
            bucket_name = config["gcs"]["bucket_name"]
            gcs_folder_name = config["gcs"]["gcs_folder_name"]

            print(f"THIS SIT THE FOLDER FNAME: {gcs_folder_name}")

            print(f"Base URL: {base_url}")
            print(f"Number of Users per call: {number_of_users}")
            print(f"Number of API Calls: {number_of_calls}")
            print(f"Batch Size for DB insert: {batch_size}")
            print(f"Schema from config: {schema}")
            print(f"DuckDB Table Name: {table_name}")
        

            print(f"All Columns found in schema: {all_columns}")
            print(f"Columns to exclude for DuckDB: {duck_db_exclusions}")
           
            print(table_creation_string)
            print(f"THE TABLE INSERTION STRING {table_insertion_string}")
        
        except Exception as e:
            print(f"something wrong: {e}")



    with duckdb.connect("database.db") as con:
        con.sql(table_creation_string)
    
    
        batch = []

        async with aiohttp.ClientSession() as session:
            api_client = ApiClient(session, base_url, all_columns)

            for call in range(number_of_calls):

                df = await(api_client.get_users(number_of_users, all_columns))                
                batch.append(df)

                if len(batch) >= batch_size:
                    print("sending onwards to database")
                    for item in batch:
                        con.sql(f"INSERT INTO {table_name} BY NAME SELECT {table_insertion_string} FROM item")
                    batch = []

            # flush out the cache here, 
            for item in batch:
                con.sql(f"INSERT INTO {table_name} BY NAME SELECT {table_insertion_string} FROM item")
            print("sending remaining data")
        
        print(len(batch))


        if has_reached_threshold(con,gcs_file_rows_threshold, table_name):
            export_duck_db_to_gcs(con, table_name,bucket_name, gcs_folder_name)
        else:
            print("not enough data")


        con.sql(f"SELECT * FROM {table_name}").show()




asyncio.run(main())
