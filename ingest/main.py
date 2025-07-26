import pandas as pd
import yaml
import aiohttp
import asyncio
import duckdb

from api import ApiClient
from db import create_duck_db_table_creation_string_from_schema, create_duck_db_table_insertion_string_from_schema


async def main(): 

    with open("config.YAML", "r") as file:
        config = yaml.safe_load(file)

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


    with duckdb.connect("/data/database.db") as con:
        con.sql(table_creation_string)

        async with aiohttp.ClientSession() as session:
            api_client = ApiClient(session, base_url, all_columns)
            tasks = [
                    asyncio.create_task(api_client.get_users(number_of_users, all_columns))
                    for task in range(number_of_calls)
                ]

            batch = []

            for future in asyncio.as_completed(tasks):
                df = await future
                if df is not None and not df.empty:
                    batch.append(df)

                if len(batch) >= batch_size:
                    combined_df = pd.concat(batch)
                    con.execute(f"INSERT INTO {table_name} BY NAME SELECT {table_insertion_string} FROM combined_df")
                    batch = []

            if batch:
                combined_df = pd.concat(batch)
                con.execute(f"INSERT INTO {table_name} BY NAME SELECT {table_insertion_string} FROM combined_df")

        
        con.sql(f"SELECT * FROM {table_name}").show()


asyncio.run(main())
