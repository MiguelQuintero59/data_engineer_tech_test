import csv
import io
import logging
import apache_beam as beam

from typing import Tuple
from apache_beam import pvalue
from collections import defaultdict
from apache_beam import PCollection
from datetime import datetime, timezone

logger = logging.getLogger(__name__)


class ParserDLQ(beam.DoFn):
    def process(self, row: str):
        try:
            row_lst = next(csv.reader(io.StringIO(row)))
        except Exception as error:
            yield pvalue.TaggedOutput(
                "dlq",
                {
                    "original_message": row,
                    "failure_type": "CSV parsing error",
                    "error_message": str(error),
                    "failed_at": datetime.now(timezone.utc).isoformat(),
                },
            )
            return

        if len(row_lst) != 4:
            yield pvalue.TaggedOutput(
                "dlq",
                {
                    "original_message": row,
                    "failure_type": "CSV parsing error",
                    "error_message": f"Expected 4 columns, got {len(row_lst)}",
                    "failed_at": datetime.now(timezone.utc).isoformat(),
                },
            )
            return

        try:
            transaction_dict = {
                "timestamp": datetime.strptime(
                    row_lst[0].strip(), "%Y-%m-%d %H:%M:%S %Z"
                ),
                "origin": row_lst[1].strip(),
                "destination": row_lst[2].strip(),
                "transaction_amount": float(row_lst[3].strip()),
            }
            yield transaction_dict
        except ValueError as error:
            yield pvalue.TaggedOutput(
                "dlq",
                {
                    "original_message": row,
                    "failure_type": "CSV parsing error",
                    "error_message": str(error),
                    "failed_at": datetime.now(timezone.utc).isoformat(),
                },
            )
            return
