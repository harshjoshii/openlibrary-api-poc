import psycopg2
from psycopg2.extras import execute_values
from psycopg2 import sql
from typing import List

from config import settings
from models import Works
from logger import logger
from db_operator import DBOperator

class InboundDBOperator(DBOperator):
    def __init__(self, database_name=settings.DATABASE_NAME):
        DBOperator.__init__(self, database_name)

    # Create schema to store data
    def initiate_schema(self):
        with self._connection.cursor() as cursor:
            logger.log(f"Initiating schema...", logger.info)
            try:
                cursor.execute("""
                                -- Create Schema
                                CREATE SCHEMA IF NOT EXISTS dbo;
                                """)
                cursor.execute("""
                                -- Create Work Table
                                CREATE TABLE IF NOT EXISTS dbo.work (
                                    key VARCHAR(20) PRIMARY KEY,
                                    title VARCHAR(200) NOT NULL,
                                    description TEXT,
                                    type VARCHAR(100) NOT NULL,
                                    revision INT NOT NULL,
                                    latest_revision INT NOT NULL,
                                    first_publish_date VARCHAR(200),
                                    created TIMESTAMP,
                                    last_modified TIMESTAMP,
                                    api_response_status VARCHAR(100)
                                );
                                """)
                cursor.execute("""
                                -- Create a map table between work and cover
                                CREATE TABLE IF NOT EXISTS dbo.work_author_map(
                                    key SERIAL PRIMARY KEY,
                                    work_key VARCHAR(20),
                                    author_key VARCHAR(20), -- This can also be a foreign key to author table. Just keeping the code short for POC
                                    FOREIGN KEY (work_key) REFERENCES dbo.work(key)
                                );           
                """)
                logger.log("  Done!", logger.info)
            except (Exception, psycopg2.Error) as error:
                logger.log("  Error initiating schema!", logger.error, error)

    # Using bulk inserts to optimize the performance instead of single inserts
    def bulk_insert_work_author_map(self, work: Works):
            query = sql.SQL("""INSERT INTO dbo.work_author_map(
                                        work_key, 
                                        author_key
                                )
                                VALUES %s""")
            logger.log(f"Inserting work author map {work.title}...", logger.info)
            with self._connection.cursor() as cursor:
                try:
                    execute_values(cursor, 
                                    query,
                                    [[work.key.split('/')[2], author_type.author.key.split('/')[2]] for author_type in work.authors],
                                    page_size=100)
                    logger.log("  Done! ", logger.info)
                except psycopg2.Error as error:
                    logger.log(f"  Error inserting work author map! Error code: {error.pgcode}", logger.error, error)
                except Exception as error:
                    logger.log(f"  Error occured! {repr(error)}", logger.error, error)


    # Insert work in multiples for performance
    def bulk_insert_work(self, works: List[Works]):
            query = sql.SQL("""INSERT INTO dbo.work(
                                        key, 
                                        title,
                                        description,
                                        type,
                                        revision,
                                        latest_revision,
                                        first_publish_date,
                                        created,
                                        last_modified
                                )
                                VALUES %s""")
            logger.log(f"Inserting works...", logger.info)
            with self._connection.cursor() as cursor:
                try:
                    execute_values(cursor, 
                                    query,
                                    [[work.key.split('/')[2], 
                                        work.title,
                                        work.description,
                                        work.type.key.split('/')[2],
                                        work.revision,
                                        work.latest_revision,
                                        work.first_publish_date,
                                        work.created.value,
                                        work.last_modified.value] for work in works],
                                    page_size=100)
                    logger.log("  Done!", logger.info)
                except psycopg2.Error as error:
                    logger.log(f"  Error inserting work! Error code: {error.pgcode}", logger.error, error)
                except Exception as error:
                    logger.log(f"  Error occured! Check logs.", logger.error, error)

    # Do stuff
    def process(self, works):
        self.initiate_schema()
        self.bulk_insert_work(works)
        for work in works:
            self.bulk_insert_work_author_map(work)

