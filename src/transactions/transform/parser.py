import logging
from datetime import datetime

logger = logging.getLogger(__name__)

def parse_rows(row):
    row_lst = row.split(",")

    if len(row_lst) != 4:
        logger.error(f"Invalid row format expected 4 values but got {len(row_lst)}")
        raise ValueError (f"Expected 4 values but got {len(row_lst)}")

    try:
        transaction_dict = {
            'timestamp': datetime.strptime(row_lst[0].strip(), '%Y-%m-%d %H:%M:%S %Z'),
            'origin': row_lst[1].strip(),
            'destination': row_lst[2].strip(),
            'transaction_amount': float(row_lst[3].strip())
        }
    except ValueError as error:
        logger.error("Failed to parse row: %s", row)
        logger.error("Parsing error: %s", error)
        raise

    logger.debug("Parsing CSV file: %s",transaction_dict) 
    return transaction_dict