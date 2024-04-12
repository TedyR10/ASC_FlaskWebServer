import logging
from logging.handlers import RotatingFileHandler
import time

# Set up logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Create a rotating file handler
log_file = RotatingFileHandler('webserver.log', maxBytes=1024*1024, backupCount=10)

# Create a logging format
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
formatter.converter = time.gmtime
log_file.setFormatter(formatter)
logger.addHandler(log_file)