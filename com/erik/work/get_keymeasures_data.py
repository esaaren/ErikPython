import logging
import boto3
import time
import requests
from xml.etree import ElementTree
from KeymeasuresRequestHelper import KeymeasuresRequestHelper
import pickle
import os.path

s3 = boto3.client('s3')
logging.basicConfig()
logger = logging.getLogger('keymeasure_logger')
logger.setLevel(logging.INFO)


def get_time_period(url, auth, body, geo):
    submit_action = '"http://comscore.com/DiscoverParameterValues"'
    submit_headers = {'Authorization': auth, 'content-type': 'text/xml;charset=utf-8', 'SOAPAction': submit_action}
    body = body.format(geo)
    response = requests.post(url, data=body, headers=submit_headers)
    envelope = ElementTree.fromstring(response.content)
    x = {}
    # For each EnumValue in the response which is each Id for the time period, append it to an array
    for child in envelope[0][0][0]:
        if 'EnumValue' in child.tag:
            x[int(child.attrib['Id'])] = child.attrib['Value'].lower().replace(" ", "")
    return x


def get_targets(url, auth, body, geo, time_period):
    submit_action = '"http://comscore.com/DiscoverParameterValues"'
    submit_headers = {'Authorization': auth, 'content-type': 'text/xml;charset=utf-8', 'SOAPAction': submit_action}
    body = body.format(geo, time_period)
    response = requests.post(url, data=body, headers=submit_headers)
    envelope = ElementTree.fromstring(response.content)
    x = {}
    # For each EnumValue in the response which is each Id for the time period, append it to an array
    for child in envelope[0][0][0]:
        if 'EnumValue' in child.tag:
            x[int(child.attrib['Id'])] = child.attrib['Value']. \
                lower(). \
                replace(" ", ""). \
                replace(":", ""). \
                replace("+", "plus"). \
                replace("(", ""). \
                replace(")", "")
    return x


def get_media_sets(url, auth, body, geo, time_period):
    submit_action = '"http://comscore.com/DiscoverParameterValues"'
    submit_headers = {'Authorization': auth, 'content-type': 'text/xml;charset=utf-8', 'SOAPAction': submit_action}
    body = body.format(geo, time_period)
    response = requests.post(url, data=body, headers=submit_headers)
    envelope = ElementTree.fromstring(response.content)
    x = {}
    # For each EnumValue in the response which is each Id for the time period, append it to an array
    for child in envelope[0][0][0]:
        if 'EnumValue' in child.tag:
            x[int(child.attrib['Id'])] = child.attrib['Value']. \
                lower(). \
                replace(" ", ""). \
                replace(":", ""). \
                replace("+", "plus"). \
                replace("(", ""). \
                replace(")", "")
    return x


def get_report(url, auth, body, geo, time_period, target, media_set_type, sleep_sec):
    logger.info(
        "Working on report type, geo: {}, time_period: {}, target: {}, media_set_type: {}".format(geo, time_period,
                                                                                                  target,
                                                                                                  media_set_type))
    submit_action = '"http://comscore.com/SubmitReport"'

    # Define the headers
    submit_headers = {'Authorization': auth, 'content-type': 'text/xml;charset=utf-8', 'SOAPAction': submit_action}
    ping_headers = {'Authorization': auth, 'content-type': 'text/xml;charset=utf-8'}
    fetch_headers = {'Authorization': auth, 'content-type': 'text/xml;charset=utf-8'}

    ping_body = """<?xml version="1.0" encoding="utf-8"?>
                      <soap:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
                           <soap:Body>
                               <PingReportStatus xmlns="http://comscore.com/">
                               <jobId>{}</jobId>
                               </PingReportStatus>
                           </soap:Body>
                       </soap:Envelope>"""

    fetch_body = """<?xml version="1.0" encoding="utf-8"?>
                       <soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema">
                           <soap:Body>
                               <FetchReport xmlns="http://comscore.com/">
                                   <jobId>{}</jobId>
                               </FetchReport>
                           </soap:Body>
                       </soap:Envelope>"""

    # Issue the submit request
    response = requests.post(url, data=body, headers=submit_headers)

    # Parse the SOAP Response into envelope
    envelope = ElementTree.fromstring(response.content)

    # Get the job ID from the response

    job_id = envelope[0][0][0][0].text

    if job_id:
        logger.info("Job ID Submitted: {} ".format(job_id))
    else:
        logger.error("No job ID returned, failing out")
        exit(-1)

    # Now need to ping for job status

    starttime = time.time()
    max_iter = sleep_sec
    it = 0
    not_completed = True
    while not_completed:
        # Issue job check request
        response = requests.post(url, data=ping_body.format(job_id), headers=ping_headers)
        envelope = ElementTree.fromstring(response.content)
        job_status = envelope[0][0][0][0].text
        logger.debug("Job status with id {}: {} ".format(job_id, job_status))
        if job_status == 'Completed':
            logger.info("Job {} completed.".format(job_id))
            not_completed = False
        else:
            logger.debug("Sleeping 1 second...")
            it = it + 1
            if it >= max_iter:
                break
            time.sleep(1.0 - ((time.time() - starttime) % 1.0))

    # Need to fetch the report now

    logger.info("Fetching report from job id: {}".format(job_id))

    response = requests.post(url, data=fetch_body.format(job_id), headers=fetch_headers)
    logger.debug("Report content: " + response.content)

    filename = 'keymeasures_{}_{}_{}_{}.xml'.format(geo, time_period, target, media_set_type)

    logger.info("Fetched report content")

    bucket = "torstar-data-workspace"
    path = 'data/raw/comScore'
    key = path + "/" + filename
    response = s3.put_object(Bucket=bucket,
                             Body=response.content,
                             Key=key
                             )

    logger.info("Loaded report with job id: {} to S3 raw".format(job_id))

def run(argv=None):
    helper = KeymeasuresRequestHelper()

    # Define config vars
    url = helper.url
    auth = helper.auth
    time_period_body = helper.time_period_body
    target_body = helper.target_body
    submit_body = helper.submit_body
    media_set_body = helper.media_set_body

    # Region code array, dict
    geos = {
        1: "worldwide",
        2: "canada"
    }

    # Intending on saving every time a report is completed

    if os.path.exists('save_progress'):
        with open('save_progress', 'rb') as handle:
            save_progress = pickle.load(handle)
    else:
        save_progress = []

    # Iterate over each geographical region
    for geo in geos:
        # Get the time periods for the geographical region
        time_periods = get_time_period(url, auth, time_period_body, geo)
        # Iterate over each time period
        for time_period in time_periods:
            targets = get_targets(url, auth, target_body, geo, time_period)
            for target in targets:
                media_sets = get_media_sets(url, auth, media_set_body, geo, time_period)
                for media_set in media_sets:
                    if (str(geo) + '-' + str(time_period) + '-' + str(target) + '-' + str(
                            media_set)) not in save_progress:
                        body = submit_body.format(geo, time_period, target, media_set)
                        # Get the names for each item from the dict for pretty file naming
                        get_report(url, auth, body, geos[geo], time_periods[time_period], targets[target],
                                   media_sets[media_set], 120)

                        # Save progress of the writer
                        save_progress.append(
                            str(geo) + '-' + str(time_period) + '-' + str(target) + '-' + str(media_set))

                        with open('save_progress', 'w') as f:
                            pickle.dump(save_progress, f)

                    else:
                        logger.info(
                            "Geo: {}, "
                            "TimePeriod: {}, "
                            "Target: {}, "
                            "Media Set: {} "
                            "already loaded ... skipping to next report"
                            .format(
                                geos[geo], time_periods[time_period], targets[target], media_sets[media_set]
                            )
                        )

    logger.info("Finished!")


if __name__ == '__main__':
    run()
