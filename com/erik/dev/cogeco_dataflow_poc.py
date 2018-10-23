from __future__ import absolute_import
import argparse
import logging
import re
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

""" 
    How to run via cloud shell

    The V_CUSTOMER_<date> files are in a bucket gs://erik-dataflow/
    Make sure you also have  gs://erik-dataflow/ready 
    
    On cloud shell or a CE instance I did the following to spin up a python virtual env:
    
    pip install --upgrade virtualenv
    virtualenv --python=/usr/bin/python2.7 /home/erik_saarenvirta93/erik_venv
    . /home/erik_saarenvirta93/erik_venv/bin/activate
    cd /home/erik_saarenvirta93/erik_venv/bin
    pip install apache-beam[gcp]
    mkdir /home/erik_saarenvirta93/erik_venv/tmp
    
    Now move over your two code files into: /home/erik_saarenvirta93/erik_venv/bin
    Chmod 700 <files> 
    
    Navigate to /home/erik_saarenvirta93/erik_venv/bin
    
    Now run:
    Nohup ./run_pipeline.sh &
"""


class DataIngestion:

    def parse_method(self, string_input):
        # Strip out return characters and quote characters.
        values = re.split(";",
                          re.sub('\r\n', '', re.sub(u'"', '', string_input)))

        row = dict(zip(('CUSTACCTNUMBER', 'CUSTACCTID', 'MARKETINGCATEGORY', 'CUSTOMERCATEGORY', 'DATE_PART'),
                       values))

        return row

def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--input', dest='input', required=False,
        help='Input file to read.  This can be a local file or '
             'a file in a Google Storage Bucket.',
        default='gs://erik-dataflow/ready/V_CUSTOMER*')

    parser.add_argument('--output', dest='output', required=False,
                        help='Output BQ table to write results to.',
                        default='poc.cogeco_customer')  # Change this

    # Parse arguments from the command line.
    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_args.extend([
        '--runner=DataflowRunner',
        '--project=idyllic-kit-191017',
        '--staging_location=gs://erik-dataflow/stg',  # Change this
        '--temp_location=gs://erik-dataflow/tmp',  # Change this
        '--job_name=poc-cogeco-customer',
    ])

    data_ingestion = DataIngestion()

    p = beam.Pipeline(options=PipelineOptions(pipeline_args))

    (p
     | 'Read from a File' >> beam.io.ReadFromText(known_args.input,
                                                  skip_header_lines=1)
     | 'String To BigQuery Row' >> beam.Map(lambda s:
                                            data_ingestion.parse_method(s))
     | 'Write to BigQuery' >> beam.io.Write(
                beam.io.BigQuerySink(
                    known_args.output,
                    schema='CUSTACCTNUMBER:STRING,CUSTACCTID:STRING,MARKETINGCATEGORY:STRING,CUSTOMERCATEGORY:STRING,DATE_PART:STRING',
                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                    write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE)))
    p.run().wait_until_finish()


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()