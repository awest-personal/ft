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
        schema = config["schema"]
        table_name = config["target_configs"]["duckdb"]["table_name"]
        
        all_columns = list(config["schema"]["columns"].keys())
        duck_db_exclusions = list(config["target_configs"]["duckdb"]["exclude_columns"])

        table_creation_string = create_duck_db_table_creation_string_from_schema(schema, table_name, duck_db_exclusions)
        table_insertion_string = create_duck_db_table_insertion_string_from_schema(schema, duck_db_exclusions)

    except Exception as e:
        print(f"something wrong: {e}")


    await run_ingestion_pipeline(
            db_path="/data/database.db",
            table_name=table_name,
            table_creation_string=table_creation_string
            api_config=config["api_config"]
            all_columns=all_columns
        )
        
    con.sql(f"SELECT * FROM {table_name}").show()


asyncio.run(main())
