import logging
from datetime import datetime
from logging import handlers
import os


class Logger:

    def __init__(self):
        logger = logging.getLogger('logger')
        logger.setLevel(logging.DEBUG)

        rf_handler = handlers.TimedRotatingFileHandler('logs/all.log', when='midnight', interval=1, backupCount=7,
                                                       atTime=datetime.now())
        rf_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))

        f_handler = logging.FileHandler('logs/aboveWarning.log')
        f_handler.setLevel(logging.WARNING)
        f_handler.setFormatter(
            logging.Formatter("%(asctime)s - %(levelname)s - %(filename)s[:%(lineno)d] - %(message)s"))

        logger.addHandler(rf_handler)
        logger.addHandler(f_handler)
        self.logger = logger

    def get_logger(self):
        return self.logger

if not os.path.exists("./logs/"):
    os.mkdir("logs/")
LOGGER = Logger().get_logger()
