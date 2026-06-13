import json
import apache_beam as beam

from apache_beam.io.filesystem import CompressionTypes
from apache_beam.options.pipeline_options import PipelineOptions
from transactions.config import input_file, output_file
from transactions.config_logger import setup_logger
from transactions.transform.parser import ParserDLQ
from transactions.transform.transformation import Aggregations
from transactions.transform.composite import CompositeTransform

logger = setup_logger()


def run():
    logger.info("Started Apache Beam Pipeline")

    pipeline_options = PipelineOptions(
        [
            "--runner=DirectRunner",
        ]
    )

    try:
        with beam.Pipeline(options=pipeline_options) as pipeline:
            logger.info("Extraction phase")

            extract = pipeline | "Read Dataset from GCS" >> beam.io.ReadFromText(
                input_file, skip_header_lines=1
            )

            parsed = extract | "Parse transaction rows" >> beam.ParDo(
                ParserDLQ()
            ).with_outputs("dlq", main="valid")

            result_outputs = parsed.valid
            result_failures = parsed.dlq

            aggregations = Aggregations()
            transformation = result_outputs | CompositeTransform(
                aggregations=aggregations
            )

            load = (
                transformation
                | "Format into JSON" >> beam.Map(aggregations.format_into_json)
                | "Save output file"
                >> beam.io.WriteToText(
                    output_file,
                    shard_name_template="",
                    compression_type=CompressionTypes.GZIP,
                )
            )

            dlq_load = (
                result_failures
                | "Format DLQ records into JSON" >> beam.Map(json.dumps)
                | "Save DLQ output file"
                >> beam.io.WriteToText(
                    "output/dead_letter.jsonl.gz",
                    shard_name_template="",
                    compression_type=CompressionTypes.GZIP,
                )
            )
        logger.info("Finished Apache Beam pipeline")
    except Exception as e:
        logger.error("Transactions etl pipeline failed: %s", e)
        raise


if __name__ == "__main__":
    run()
