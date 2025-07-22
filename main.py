import yaml
import aiohttp
import asyncio

from api import ApiClient

async def main(): 
    with open("config.YAML", "r") as config:
        try:
            config = yaml.safe_load(config)
        except yaml.YAMLError as e:
            print(e)

    base_url = config["api_config"]["base_url"]
    number_of_users = config["api_config"]["number_of_users"]
    print(base_url, number_of_users)

    async with aiohttp.ClientSession() as session:
        api_client = ApiClient(session, base_url)

        d1 = await api_client.get_data(number_of_users)
        d2 = await api_client.get_data(40)
        d3 = await api_client.get_data(10)
        d4 = await api_client.get_data(4)


        print(len(d1),len(d2),len(d3),len(d4))

asyncio.run(main())
