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




    