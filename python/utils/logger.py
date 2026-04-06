from loguru import logger
import sys
from config.settings import LOG_LEVEL


logger.remove()
logger.add(sys.stdout,
               format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {name} | {message}",
               level=LOG_LEVEL)


def get_logger(name : str, level: str = "INFO"):
    """Get a logger with the specified name and level.
    Args:
        name (str): The name of the logger.
    
    Log level is set from .env file, default is INFO."""
    return logger.bind(name=name)
