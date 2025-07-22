import aiohttp
import asyncio


class ApiClient:

    def __init__(self, session, base_url):

        self.session = session
        self.base_url = base_url


    async def get_data(self, quantity):

        users_url = f"{self.base_url}users"
        params = {"_quantity": quantity}

        async with self.session.get(users_url, params=params) as resp:
            resp.raise_for_status() # check resp code ici

            data = await resp.json()

            return data.get("data", [])


    