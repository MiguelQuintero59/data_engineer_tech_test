import json
from typing import Dict
from datetime import date

def filter_amount(transaction_row):
    return transaction_row['transaction_amount']>20

def filter_date(transaction_row):
    return transaction_row['timestamp'].date() >= date(2010,1,1)

def format_into_json(transaction_row):
  transaction_date, transaction_amount = transaction_row
  return json.dumps({'date':transaction_date,
                     'total_amount':transaction_amount})
