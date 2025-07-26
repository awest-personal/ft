import aiohttp
import asyncio
import pandas as pd


class ApiClient:

    def __init__(self, session, base_url, ordered_columns):

        self.session = session
        self.base_url = base_url
        self.ordered_columns = ordered_columns


    async def get_users(self, quantity, ordered_columns):

        users_url = f"{self.base_url}users"
        params = {"_quantity": quantity}

        

        async with self.session.get(users_url, params = params) as resp:
            resp.raise_for_status() # check resp code ici

            data = await resp.json()
            try:
                list_of_users = data["data"]
                df = pd.DataFrame(list_of_users, columns=ordered_columns)
                return df
            except Exception as e:
                return null


async def ingest_data_from_api(con, config, api_client):
    # orchestration function for the api loading, will run asynchronously

    # duckdb stuff
    table_name = config["target_configs"]["duckdb"]["table_name"]
    all_columns = list(config["schema"]["columns"].keys())
    duck_db_exclusions = list(config["target_configs"]["duckdb"]["exclude_columns"])

    # api stuff
    batch_size = config["api_config"]["batch_size"]
    number_of_calls = config["api_config"]["number_of_calls"]
    number_of_users = config["api_config"]["number_of_users"]
    base_url = config["api_config"]["base_url"]


    batch = []

    for call in range(number_of_calls):
        df = await(api_client.get_users(number_of_users, all_columns))
        if df:
            batch.append(df)

        if len(batch) >= batch_size:
            print("sending onwards to database")
            for item in batch:
                con.sql(f"INSERT INTO {table_name} BY NAME SELECT {table_insertion_string} FROM item")
            batch = []
    
    if batch:
        for item in batch:
            con.sql(f"INSERT INTO {table_name} BY NAME SELECT {table_insertion_string} FROM item")
            print("sending remaining data")
            
    print("ingestion complete")





    