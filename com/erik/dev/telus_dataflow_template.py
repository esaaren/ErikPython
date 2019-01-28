from __future__ import absolute_import
import argparse
import logging
import re
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions


class DataIngestion:

    def parse_method(self, string_input):
        # Strip out return characters and quote characters.
        values = re.split(",",
                          re.sub('\r\n', '', re.sub(u'"', '', string_input)))

        row = dict(zip(('timestamp', 'event', 'msidn', 'imsi', 'imei', 'cellid','lat', 'lon','radius'),
                       values))

        return row


class TelusOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
      # Use add_value_provider_argument for arguments to be templatable
      # Use add_argument as usual for non-templatable arguments
      parser.add_value_provider_argument(
          '--input',
          default='gs://telus_poc_ready/sample_geo_data*',
          help='Path of the file to read from')
      parser.add_argument(
          '--output',
          required=True,
          help='Output file to write results to.')

def run(argv=None):

    data_ingestion = DataIngestion()
    pipeline_options = PipelineOptions(['--output', 'poc.telus_geospatial'])
    p = beam.Pipeline(options=pipeline_options)
    telus_options = pipeline_options.view_as(TelusOptions)

    (p
     | 'Read from a File' >> beam.io.ReadFromText(telus_options.input,
                                                  skip_header_lines=1)
     | 'String To BigQuery Row' >> beam.Map(lambda s:
                                            data_ingestion.parse_method(s))
     | 'Write to BigQuery' >> beam.io.Write(
                beam.io.BigQuerySink(
                    cogeco_options.output,
                    schema='TIMESTAMP:STRING, EVENT:STRING, MSIDN:STRING, IMSI:STRING, IMEI:STRING, CELLID:STRING, LAT:FLOAT, LON:FLOAT, RADIUS:FLOAT',
                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                    write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE)))
    p.run().wait_until_finish()


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()