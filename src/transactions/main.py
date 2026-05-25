import apache_beam as beam
from apache_beam.io.filesystem import CompressionTypes
from apache_beam.options.pipeline_options import PipelineOptions

from transactions.config import input_file, output_file
from transactions.config_logger import setup_logger
from transactions.transform.parser import parse_rows
from transactions.transform.composite import CompositeTransform
from transactions.transform.transformation import format_into_json 


logger = setup_logger()

def run():
    logger.info("Started Apache Beam Pipeline")
    pipeline_options = PipelineOptions([
        "--runner=DirectRunner",
    ])

    try:
        with beam.Pipeline(options=pipeline_options) as pipeline:
            logger.info("Extraction phase")
            extract = (pipeline 
                | "Read Dataset from GCS" >> beam.io.ReadFromText(input_file,skip_header_lines=1)
                | "Parse transaction rows" >> beam.Map(parse_rows)
            ) 
            
            transformation = (extract
                | CompositeTransform()
            )
            
            load = (transformation
                | "Format into JSON" >> beam.Map(format_into_json)
                | "Save output file" >> beam.io.WriteToText(output_file,shard_name_template="",compression_type=CompressionTypes.GZIP)
            )
        logger.info("Finished Apache Beam pipeline")
    except Exception as e:
        logger.error("Transactions etl pipeline failed: %s", e)
        raise

if __name__ == "__main__":
    run()