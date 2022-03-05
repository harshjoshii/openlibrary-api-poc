import psycopg2

from config import settings
from db_operator import DBOperator
from logger import logger

class OutboundDBOperator(DBOperator):
    def __init__(self, database_name=settings.DATABASE_NAME):
        DBOperator.__init__(self, database_name)
    
    #  Get work author map records from db
    def fetch_works_with_authors(self):
        records = []
        with self._connection.cursor() as cursor:
            try:
                logger.log("Retrieving works & authors from db...", logger.info)                
                cursor.execute("""
                                -- Get works and authors in json
                                SELECT 
                                    JSON_BUILD_OBJECT(
                                    'key', w.key,
                                    'title', w.title,
                                    'description', w.description,
                                    'type', JSON_BUILD_OBJECT('key', w.type),
                                    'revision', w.revision,
                                    'latest_revision', w.latest_revision,
                                    'first_publish_date', w.first_publish_date,
                                    'created', JSON_BUILD_OBJECT(
                                        'type', '/type/datetime',
                                        'value', w.created),
                                    'last_modified', JSON_BUILD_OBJECT(
                                        'type', '/type/datetime',
                                        'value', w.last_modified),
                                        'authors', JSON_AGG(JSON_BUILD_OBJECT(
                                        'author', JSON_BUILD_OBJECT(
                                            'key', wam.author_key),
                                        'type', JSON_BUILD_OBJECT(
                                            'key', '/type/author_role')            
                                        ))
                                    )
                                FROM
                                    dbo.work w
                                    INNER JOIN dbo.work_author_map wam ON wam.work_key = w.key
                                WHERE
                                    w.api_response_status IS NULL or w.api_response_status <> '200'
                                GROUP BY
                                    w.key,
                                    w.title,
                                    w.description,
                                    w.type,
                                    w.revision,
                                    w.latest_revision,
                                    w.first_publish_date,
                                    w.created,
                                    w.last_modified         
                                """)
                records = cursor.fetchall()
                description = list(cursor.description)
                logger.log("  Done!", logger.info)
            except (Exception, psycopg2.Error) as error:
                logger.log("  Error retrieving works & authors!", logger.error, error)
        return records

    # Do Stuff
    def process(self):
        return self.fetch_works_with_authors()

