
import logging
from datetime import date

class Logger:

    def __init__(self):
        # Logging can also be configured from the .yaml file or using a decorator but for simplicity of this POC, 
        # using simple logging in line without configuration file
        self.__filename = str(date.today())+".log"
        logging.basicConfig(filename=self.__filename, 
                            filemode="a", 
                            format="%(asctime)s : %(levelname)s : [%(filename)s:%(lineno)s - %(funcName)s()] : %(message)s",
                            datefmt='%Y-%m-%d %H:%M:%S', 
                            level=logging.INFO)
        self.error = logging.error
        self.info = logging.info
        self.warn = logging.warn
    
    # Simple logging function
    def log(self, message, logging, exception = None):
        print(message)
        logging(message+" "+ repr(exception) if exception else "")

logger = Logger()