import asyncio
from aiohttp import ClientSession
from aiohttp.web import HTTPException
from pydantic import parse_obj_as, ValidationError

from models import Works
from logger import logger
from config import settings


API_URL = "https://openlibrary.org"


class APIDataRetriver:
    def __init__(self):
        self.endpoints = [
                        "/works/OL45883W.json",
                        "/works/OL16923512W.json",
                        "/works/OL957949W.json",
                        "/works/OL2231866W.json",
                        "/works/OL1881592W.json",
                        "/works/OL4304829W.json",
                        "/works/OL2668678W.json",
                        "/works/OL15202029W.json"
                    ]
        self.__works_json = []
        self.__works_model = []              
  

    # Function to send requests in async fashion
    async def get_work(self, session, url):
        try:
            logger.log(f"  {url}", logger.info)
            async with session.get(url, timeout=settings.REQUEST_TIMEOUT, allow_redirects=False) as resp:                
                work = await resp.json()
                return work
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
            for endpoint in self.endpoints:
                endpoint_url = API_URL + endpoint
                tasks.append(asyncio.ensure_future(self.get_work(session, endpoint_url)))
            self.__works_json = await asyncio.gather(*tasks)
            logger.log(f"  Done!", logger.info)

    # Parse json to pydantic model. This can also be achieved in async fashion to optimize performance
    def parse_work_json_to_model(self):
        for work in self.__works_json:
            try:    
                self.__works_model.append(parse_obj_as(Works, work)) # Using Pydantic to parse the json object to its model. Validations can also be applied if needed.
                logger.log(f"  Work parsed. Key: {self.__works_model[-1].key} Title: {self.__works_model[-1].title}", logger.info)
            except ValidationError as e:
                if work:
                    logger.log(f"  Data validation error of work with key: {work['key']}", logger.error, e)
                return None

    # Process and get the list of works
    def process(self):
        asyncio.run(self.send_requests())
        self.parse_work_json_to_model()
        return self.__works_model

    def __del__(self):
        del self.__works_json
        del self.__works_model
        
            
# retriever = APIDataRetriver()
# works = retriever.process()
# print(works[0])

