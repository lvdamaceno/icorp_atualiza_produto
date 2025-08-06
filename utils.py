import logging
import os


def logging_config():
    level = os.getenv('LEVEL_LOGGING')
    if level == '0':
        logging.basicConfig(format='%(asctime)s %(message)s', level=logging.DEBUG)
    elif level == '1':
        logging.basicConfig(format='%(asctime)s %(message)s', level=logging.INFO)