import aiohttp
import asyncio
import logging


class ApiError(Exception): 
    "can expand on this later, basically custom exception to hold api stuff"
    pass


class ApiClient:
    def __init__(self, session, base_url):
        self.session = session
        self.base_url = base_url 


    async def get_users(self, quantity: int) -> [dict]:
        """
        Fetches users from the API, returns a pandas dataframe.
        """
        users_url = f"{self.base_url}users"
        params = {"_quantity": quantity}

        async with self.session.get(users_url, params = params) as resp:
            await asyncio.sleep(1)
            resp.raise_for_status() # check resp code ici

            logging.info("making api call")
            data = await resp.json()

            if "data" not in data:
                raise ApiError(f"response missing data for some reason: {data}")
            
            try:
                return data["data"]
            
            except Exception as e:
                raise ApiError(f"couldn't return data: {e}")


