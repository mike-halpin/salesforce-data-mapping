import logging
import time

# ANSI escape sequences for colors
COLORS = {
    'WARNING': '\033[93m',  # Yellow
    'INFO': '\033[92m',    # Green
    'DEBUG': '\033[94m',   # Blue
    'CRITICAL': '\033[91m',# Red
    'ERROR': '\033[91m',   # Red
    'ENDC': '\033[0m'      # End color
}
class ColoredFormatter(logging.Formatter):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def format(self, record):
        colored_record = record
        prefix = COLORS.get(record.levelname, COLORS['ENDC'])
        colored_record.msg = f"{prefix}{record.msg}{COLORS['ENDC']}"
        return super().format(colored_record)

def get_logger(module_name, file_name=f'Log_{(time.strftime("%Y%m%d-%H%M%S")}.log')):
    # Create a logger
    logger = logging.getLogger(module_name)
    if not logger.handlers:
        # Set the log level
        logger.setLevel(logging.DEBUG)
        # Create a console handler
        console_handler = logging.StreamHandler()
        # Set the level for this handler
        console_handler.setLevel(logging.DEBUG)
        # Create a formatter and set the formatter for the console handler.
        console_format = ColoredFormatter('%(asctime)s - %(levelname)s - %(message)s')
        console_handler.setFormatter(console_format)
        # Add the console handler to the logger
        logger.addHandler(console_handler)

        # Create a file handler
        file_handler = logging.FileHandler(file_name)
        # Set the level for this handler
        file_handler.setLevel(logging.DEBUG)
        # Create a formatter and set the formatter for the file handler.
        file_format = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(file_format)
        # Add the file handler to the logger
        logger.addHandler(file_handler)

    logger.debug("This is a debug message")
    logger.info("This is an info message")
    logger.warning("This is a warning message")
    logger.error("This is an error message")
    logger.critical("This is a critical message")
    logger.info("Logging now configured.")

    return logger

