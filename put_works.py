import asyncio
from aiohttp import ClientSession
from aiohttp.web import HTTPException

from logger import logger
from config import settings


API_URL = "https://jsonbase.com"


class APIDataSender:
    def __init__(self):
        self.__endpoints = []
        self.__works_json_out = []
        self.__works_json_in = []            

    # Function to send requests in async fashion
    async def put_work(self, session, url, json):
        try:
            logger.log(f"  {url}", logger.info)
            async with session.put(url, 
                                json=json, 
                                timeout=settings.REQUEST_TIMEOUT, 
                                headers={"content-type": "application/json"},
                                allow_redirects=False) as resp:                
                work = await resp.json()
                return [work, resp.status]
        except asyncio.exceptions.TimeoutError as e:
            logger.log(f"Timeout Error. Endpoint: {url}", logger.error, e)
        except HTTPException as e:
            logger.log(f"Request error with status code: {e.status_code}. Endpoint: {url}", logger.error, e)
        except Exception as e:
            logger.log(f"Error occured at endpoint {url}", logger.error, e)

    # A function utilizing aiohttp to generate async calls
    async def send_requests(self):
        logger.log("Sending out requests...", logger.info)
        async with ClientSession() as session:
            tasks = []
            for i, endpoint in enumerate(self.__endpoints):
                endpoint_url = API_URL + endpoint
                tasks.append(asyncio.ensure_future(self.put_work(session, endpoint_url, self.__works_json_out[i])))
            self.__works_json_in = await asyncio.gather(*tasks)
            logger.log(f"  Done!", logger.info)
            return self.__works_json_in


    # Use json to get list of endpoints
    def get_endpoints(self):
        for record in self.__works_json_out:
            self.__endpoints.append("/work/"+record[0]["key"])

    # Do stuff
    def process(self, works_json):
        self.__works_json_out = works_json
        self.get_endpoints()
        return asyncio.run(self.send_requests())

    def __del__(self):
        del self.__works_json_out
        del self.__works_json_in
        