from logger import logger
from get_works import APIDataRetriver
from store_works import InboundDBOperator
from retrieve_works import OutboundDBOperator
from put_works import APIDataSender
from update_works import InboundUpdateDBOperator


def main():
    # Send out async get requests to get data
    try:
        get = APIDataRetriver()
        data = get.process()
    except Exception as e:
        logger("Error getting data!", logger.error, e)

    # Store data in bulk to db
    try:
        store = InboundDBOperator()
        data = store.process(data)
    except Exception as e:
        logger("Error storing data!", logger.error, e)

    # Retrive data from db
    try:
        retrieve = OutboundDBOperator()
        data = retrieve.process()
    except Exception as e:
        logger("Error retrieving data!", logger.error, e)

    # Send out async put requests to send data
    try:
        send = APIDataSender()
        data = send.process(data)
    except Exception as e:
        logger("Error sending data!", logger.error, e)

    # Update the db based on the statuses of the responses
    try:
        update = InboundUpdateDBOperator()
        update.process(data)
    except Exception as e:
        logger("Error updating data!", logger.error, e)


if __name__ == "__main__":
    main()