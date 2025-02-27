import logging

# Configure logging
def setup_logging():
    """Configure the application-wide logging."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler("deltatask.log"),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger("DeltaTask")