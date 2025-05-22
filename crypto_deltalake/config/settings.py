import logging
from dataclasses import dataclass
from os import getenv
from typing import Optional

from dotenv import load_dotenv

load_dotenv()


@dataclass
class Settings:
    """Application settings with validation"""
    # Logging
    log_file: str = "./crypto_deltalake.log"
    log_format: str = "[{name}]-[%(levelname)s]-[%(asctime)s]-[%(message)s]"
    log_to_file: bool = True
    log_to_terminal: bool = True
    file_level: int = logging.DEBUG
    terminal_level: int = logging.INFO

    # API
    api_key: Optional[str] = None

    # Table paths
    table_liquidation_path: str = "tables/liquidation"
    table_price_path: str = "tables/prices"

    # WebSocket
    binance_websocket_url: str = "wss://fstream.binance.com/ws"

    # REST API
    binance_api_url: str = "https://api.binance.com/api/v3"

    # Price collection
    price_collection_interval: int = 60  # seconds

    def __post_init__(self):
        # Load from environment
        self.log_to_file = getenv("LOG_TO_FILE", "true").lower() == "true"
        self.log_to_terminal = getenv("LOG_TO_TERMINAL", "true").lower() == "true"
        self.api_key = getenv("API_KEY")


settings = Settings()


def get_logger(name: str) -> logging.Logger:
    """Create configured logger instance"""
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    formatter = logging.Formatter(settings.log_format.format(name=name))

    # Clear existing handlers
    logger.handlers.clear()

    if settings.log_to_file:
        file_handler = logging.FileHandler(settings.log_file, mode="a")
        file_handler.setLevel(settings.file_level)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    if settings.log_to_terminal:
        console_handler = logging.StreamHandler()
        console_handler.setLevel(settings.terminal_level)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

    return logger