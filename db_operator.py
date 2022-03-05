import psycopg2

from config import settings
from logger import logger

# Using for connection. Keeping it short for the POC.
class DBOperator:
    def __init__(self, database_name=settings.DATABASE_NAME): 
        self._connection = None
        try:       
            logger.log("Connecting to database...", logger.info)
            self._connection = psycopg2.connect(database=database_name,
                                    user=settings.DATABASE_USERNAME,
                                    password=settings.DATABASE_PASSWORD,
                                    host=settings.DATABASE_HOSTNAME, 
                                    port=settings.DATABASE_PORT)

            self._connection.autocommit = True #Setting it to true just for this POC
            with self._connection.cursor() as cursor:
                cursor.execute("SELECT version();")
                record = cursor.fetchone()
                logger.log(f"  Connection establised!", logger.info)
        except (Exception, psycopg2.Error) as error:
            logger.log("  Error establising connection to PostgreSQL instance.", logger.error, error)

    def __del__(self):
        if(self._connection != None):
            self._connection.close()
