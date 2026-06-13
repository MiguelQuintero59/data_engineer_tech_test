import unittest
import apache_beam as beam
from transactions.transform.transformation import Aggregations
from transactions.transform.composite import CompositeTransform

from transactions.config_logger import setup_logger
from transactions.transform.parser import ParserDLQ
from apache_beam.testing.test_pipeline import TestPipeline as BeamTestPipeline
from apache_beam.testing.util import assert_that, equal_to

logger = setup_logger()


class TestBeam(unittest.TestCase):
    def test_transform_dataset(self):
        expected = [("2017-03-18", 2102.22), ("2017-08-31", 13700000023.08)]

        input_data = [
            "2017-03-18 14:10:44 UTC,wallet00001866cb7e0f09a890,wallet00000e719adfeaa64b5a,2102.22",
            "2017-08-31 17:00:09 UTC,wallet00001e494c12b3083634,wallet00005f83196ec58e4ffe,13700000023.08",
            "2017-01-01 04:22:23 UTC,wallet00000e719adfeaa64b5a,wallet00001e494c12b3083634,19.95",
            "2009-01-09 02:54:25 UTC,wallet00000e719adfeaa64b5a,wallet00001866cb7e0f09a890,1021101.99",
        ]

        with BeamTestPipeline() as p:
            logger.info("Extraction phase")
            extract = p | "Testing data" >> beam.Create(input_data)
            parsed = extract | "Parse transaction rows" >> beam.ParDo(
                ParserDLQ()
            ).with_outputs("dlq", main="valid")

            result_outputs = parsed.valid
            result_failures = parsed.dlq

            aggregations = Aggregations()
            transformation = result_outputs | CompositeTransform(aggregations=aggregations)
            assert_that(transformation, equal_to(expected), label="CheckOutput")


if __name__ == "__main__":
    unittest.main()
