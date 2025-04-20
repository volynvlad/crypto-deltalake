import logging
from os import getenv

from dotenv import load_dotenv

load_dotenv()


LOG_FILE: str = "./crypto_deltalake.log"
LOG_FORMAT: str = "[{name}]-[%(levelname)s]-[%(asctime)s]-[%(message)s]"
IS_LOG_TO_FILE: bool = getenv("LOG_TO_FILE", "true").lower() == "true"
IS_LOG_TO_TERMINAL: bool = getenv("LOG_TO_TERMINAL", "True").lower() == "true"
FILE_LEVEL: int = logging.DEBUG
TERMINAL_LEVEL: int = logging.INFO


API_KEY = getenv("API_KEY", "")
TABLE_LIQUIDATION_PATH = "tables/liquidation"


def get_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    formatter = logging.Formatter(LOG_FORMAT.format(name=name))
    if IS_LOG_TO_FILE:
        file_handler = logging.FileHandler(LOG_FILE, mode="a")
        file_handler.setLevel(FILE_LEVEL)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    if IS_LOG_TO_TERMINAL:
        console_handler = logging.StreamHandler()
        console_handler.setLevel(TERMINAL_LEVEL)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)
    return logger

