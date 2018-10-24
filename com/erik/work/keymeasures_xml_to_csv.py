import logging
from xml.etree import ElementTree
import boto3

s3 = boto3.client('s3')
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def run(argv=None):
    """   Use this only when on lambda
    bucket_name = event['Records'][0]['s3']['bucket']['name']
    file_key = event['Records'][0]['s3']['object']['key']
    logger.info('Reading {} from {}'.format(file_key, bucket_name))
    # get the object
    obj = s3.get_object(Bucket=bucket_name, Key=file_key)
    x = obj['Body'].read()
    root = ElementTree.fromstring(x)
    """

    # Below for local
    tree = ElementTree.parse("testfile")
    root = tree.getroot()

    print (root[0][0][0][0][3][0][0])

    header = 'web_id|media_type|'

    entire_file = ''

    for header_element in root[0][0][0][0][3][0][0]:
        if 'TD' in header_element.tag:
            header += header_element.text.lower().replace(" ", "_")
            header += '|'

    header += 'source_system'
    header += '\n'

    entire_file += header

    for table_element in root[0][0][0][0][3][1]:
        row_string = ''
        for table_row in table_element:
            if 'TH' in table_row.tag:
                row_string += table_row.attrib['web_id']
                row_string += '|'
                row_string += table_row.text
                row_string += '|'
            if 'TD' in table_row.tag:
                row_string += table_row.text
                row_string += '|'
        row_string += 'keymeasure'
        row_string += '\n'
        entire_file += row_string.encode('utf-8')

    filename = 'test_xml_to_csv.csv'
    # filename = file_key.split('data/raw/comScore/')[1].replace('.xml', '.csv')
    bucket = "torstar-data-workspace"
    path = 'data/transformed/comScore'
    key = path + "/" + filename
    response = s3.put_object(Bucket=bucket,
                             Body=entire_file,
                             Key=key
                             )


if __name__ == '__main__':
    run()
