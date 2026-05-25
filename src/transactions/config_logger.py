import logging
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent  

def setup_logger():
    logging.getLogger("apache_beam").setLevel(logging.ERROR)
    logging.getLogger("PrismRunner").setLevel(logging.ERROR)
    logging.getLogger("PrismJobServer").setLevel(logging.ERROR)
    logging.getLogger("google.auth.default").setLevel(logging.ERROR)

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    if logger.handlers:
        return logger

    log_dir = PROJECT_ROOT / "logs" 
    log_dir.mkdir(exist_ok=True)
    log_file = log_dir / "pipeline.log"

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    console.setFormatter(formatter)
    logger.addHandler(console)

    file_log = logging.FileHandler(log_file, mode="w")
    file_log.setLevel(logging.INFO)
    file_log.setFormatter(formatter)
    logger.addHandler(file_log)
    
    return logger