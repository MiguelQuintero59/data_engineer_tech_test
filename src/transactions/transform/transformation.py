import json

from typing import Dict
from datetime import date
from abc import ABC, abstractmethod


class TransactionsFilters(ABC):
    @abstractmethod
    def filter_amount(self, transaction_row):
        pass

    @abstractmethod
    def filter_date(self, transaction_row):
        pass


class TransactionsTransformations(ABC):
    @abstractmethod
    def group_date_amount(self, transaction_row):
        pass

    @abstractmethod
    def format_into_json(self, transaction_row):
        pass


class Aggregations(TransactionsFilters, TransactionsTransformations):
    def filter_amount(self, transaction_row):
        return transaction_row["transaction_amount"] > 20

    def filter_date(self, transaction_row):
        return transaction_row["timestamp"].date() >= date(2010, 1, 1)

    def group_date_amount(self, transaction_row):
        return (
            transaction_row["timestamp"].date().isoformat(),
            transaction_row["transaction_amount"],
        )

    def format_into_json(self, transaction_row):
        transaction_date, transaction_amount = transaction_row
        return json.dumps(
            {"date": transaction_date, "total_amount": transaction_amount}
        )
