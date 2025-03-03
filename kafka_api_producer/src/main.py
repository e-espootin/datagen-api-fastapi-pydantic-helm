from api_factory import ApiFactory
from utils.logger import setup_logger
from dotenv import load_dotenv
import os
#
logger = setup_logger()

# Load environment variables from .env file
load_dotenv()


def main():
    try:
        # if parameter is null, pass kubernetes env values
        dsname = os.getenv("DSNAME", "taxitripszz")
        logger.debug(f"Debug >>>> dsname: {dsname}")

        logger.info("Kafka API Producer is running")
        logger.info(f"Dataset name: {dsname}")

        logger.info(ApiFactory().list_handler(dsname=dsname))
    except Exception as e:
        logger.error(f"Error: {e}")
        return True


if __name__ == "__main__":
    main()
