import aiohttp
import asyncio
import pandas as pd


class ApiError(Exception):
    "can expand on this later, basically custom exception to hold api stuff"
    pass


class ApiClient:
    def __init__(self, session, base_url, ordered_columns):
        self.session = session
        self.base_url = base_url
        self.ordered_columns = ordered_columns 


    async def get_users(self, quantity: int):
        """
        Fetches users from the API, returns a pandas dataframe.
        """
        users_url = f"{self.base_url}users"
        params = {"_quantity": quantity}

        async with self.session.get(users_url, params = params) as resp:
            resp.raise_for_status() # check resp code ici

            data = await resp.json()
            if "data" not in data:
                raise ApiError(f"response missing data for some reason: {data}")
            try:
                list_of_users = data["data"]
                df = pd.DataFrame(list_of_users, columns=ordered_columns)
                return df
            except Exception as e:
                raise e


async def ingest_data_from_api(db_path: str, table_name: str, table_creation_string: str, api_config: dict, all_columns: list):
    """
    Function to orchestrate the data ingestion pipeline. Will create table, concurrently fetch api data and then insert it in batches.
    """
    
    base_url = api_config["base_url"]
    number_of_users = api_config["number_of_users"]
    number_of_calls = api_config["number_of_calls"]
    batch_size = api_config["batch_size"]

    with duckdb.connect(db_path) as con:
        con.sql(table_creation_string)
        logging.info(f"tbale '{table_name}' created or already exists.")

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

    logging.info("ingestion pipeline done")



    