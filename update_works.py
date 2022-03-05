import psycopg2
from psycopg2.extras import execute_values
from psycopg2 import sql

from config import settings
from logger import logger
from db_operator import DBOperator

class InboundUpdateDBOperator(DBOperator):

    def __init__(self, database_name=settings.DATABASE_NAME):
        DBOperator.__init__(self, database_name)
        self.__work_statuses = []

    # Update work status in multiples for performance
    def bulk_update_work_status(self):
            query = sql.SQL("""
                                UPDATE 
                                    dbo.work
                                SET 
                                    api_response_status = data.status
                                FROM 
                                    (VALUES %s) AS data (key, status)
                                WHERE 
                                    dbo.work.key = data.key
                            """)
            logger.log(f"Updating work response status...", logger.info)
            with self._connection.cursor() as cursor:
                try:
                    execute_values(cursor, 
                                    query,
                                    self.__work_statuses,
                                    page_size=100)
                    logger.log("  Done!", logger.info)
                except psycopg2.Error as error:
                    logger.log(f"  Error updating work response status! Error code: {error.pgcode}", logger.error, error)
                except Exception as error:
                    logger.log(f"  Error occured! Check logs.", logger.error, error)
    
    # Parsing for validations 
    def parse_work_statuses(self, works_json):
        for work in works_json:
            self.__work_statuses.append((work[0][0]['key'], work[1]))

    # Do stuff
    def process(self, works_json):
        self.parse_work_statuses(works_json)
        self.bulk_update_work_status()

    def __del__(self):
        del self.__work_statuses