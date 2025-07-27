import pandas as pd
import yaml
import aiohttp
import asyncio
import duckdb
import logging


from api import ApiClient, ApiError
from db import create_table_from_schema, insert_data_into_duckdb



async def ingestion_pipeline(con: duckdb.DuckDBPyConnection, api_client: ApiClient, config: dict):
    """
    Function to orchestrate the data ingestion pipeline. Will create table, concurrently fetch api data and then insert it in batches.
    """

    api_config = config["api_config"]
    columns = config["duckdb"]["schema"]
    print(f"THESE ARE THE COLUMNS: {columns}")
    table_name = config["duckdb"]["table_name"]

    user_quantity = api_config["number_of_users"]
    concurrency_limit = api_config["concurrency_limit"]
    number_of_calls = api_config["number_of_calls"]
    batch_size = api_config["batch_size"]
    
    semaphore = asyncio.Semaphore(concurrency_limit)
    tasks = []

    async def worker(user_quantity):
        async with semaphore:
            user_results = await api_client.get_users(user_quantity)
            if user_results:
                df = pd.DataFrame(user_results, columns = columns)
                return df
            return None

    tasks = [asyncio.create_task(worker(user_quantity)) for task in range(number_of_calls)]

    batch = []

    for future in asyncio.as_completed(tasks):
        df = await future
        if df is not None and not df.empty:
            batch.append(df)

        if len(batch) >= batch_size:
            combined_df = pd.concat(batch)
            insert_data_into_duckdb(con, table_name, combined_df)
            batch = []
    
    if batch:
        combined_df = pd.concat(batch)
        insert_data_into_duckdb(con, table_name, combined_df)


    logging.info("pipeline complete")



async def main(): 
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    try:
        with open("config.YAML", "r") as file:
            config = yaml.safe_load(file)

        base_url = config["api_config"]["base_url"]
        duckdb_table_location = config["duckdb"]["table_location"]

        with duckdb.connect(duckdb_table_location) as con:
            create_table_from_schema(con, config)

            async with aiohttp.ClientSession() as session:
                api_client = ApiClient(session, base_url)
                await ingestion_pipeline(
                    con=con,
                    api_client=api_client,
                    config=config
                )
        
            con.sql(f"SELECT * FROM faker_api_table").show()

    except Exception as e:
        logging.error(f"something critical occurred: {e}")
            

asyncio.run(main())
