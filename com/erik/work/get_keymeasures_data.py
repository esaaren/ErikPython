import logging
import boto
from boto.s3.key import Key
import time
import requests
from xml.etree import ElementTree


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


def get_report(url, auth, body, geo, time_period, target, media_set_type):
    print "Working on report type, geo: {}, time_period: {}, target: {}, media_set_type: {}".format(geo, time_period,
                                                                                                    target,
                                                                                                    media_set_type)
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
        print "Job ID Submitted: {} ".format(job_id)
    else:
        print "No job ID returned, failing out"
        exit(-1)

    # Now need to ping for job status

    starttime = time.time()
    max_iter = 10
    it = 0
    not_completed = True
    while not_completed:
        # Issue job check request
        response = requests.post(url, data=ping_body.format(job_id), headers=ping_headers)
        envelope = ElementTree.fromstring(response.content)
        job_status = envelope[0][0][0][0].text
        print "Job status with id {}: {} ".format(job_id, job_status)
        if job_status == 'Completed':
            print "Job {} completed.".format(job_id)
            not_completed = False
        else:
            print "Sleeping 1 second..."
            it = it + 1
            if it >= max_iter:
                break
            time.sleep(1.0 - ((time.time() - starttime) % 1.0))

    # Need to fetch the report now

    print "Fetching report from job id: {}".format(job_id)

    response = requests.post(url, data=fetch_body.format(job_id), headers=fetch_headers)
    print response.content

    """  When ready to upload to s3 use the below! """

    filename = 'keymeasures_{}_{}_{}_{}.xml'.format(geo, time_period, target, media_set_type)

    keymeasures_bucket = "torstar-data-workspace"
    keymeasures_path = 'data/raw/comScore'
    conn = boto.connect_s3()
    bucket = conn.get_bucket(keymeasures_bucket)
    k = Key(bucket)
    k.key = keymeasures_path + '/' + filename
    k.set_contents_from_string(response.content)


def run(argv=None):
    # Define config vars
    url = "https://api.comscore.com/KeyMeasures.asmx"

    auth = 'Basic dHRjX2JtZW5kb25jYTp4UWFScmtiMkRESGFGMm4='

    # Define the submit body

    submit_body = """<?xml version="1.0" encoding="utf-8"?>
                    <soap:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
                        <soap:Body>
                        <SubmitReport xmlns="http://comscore.com/">
                            <query xmlns="http://comscore.com/ReportQuery">
                                <Parameter KeyId="geo" Value="{}" />
                                <Parameter KeyId="loc" Value="0" />
                                <Parameter KeyId="timeType" Value="1" />
                                <Parameter KeyId="timePeriod" Value="{}" />
                                <Parameter KeyId="targetType" Value="1" />
                                <Parameter KeyId="target" Value="{}" />
                                <Parameter KeyId="measure" Value="1" />
                                <Parameter KeyId="mediaSet" Value="4962873" />
                                <Parameter KeyId="mediaSetType" Value="{}" />
                            </query>
                        </SubmitReport>
                        </soap:Body>
                    </soap:Envelope>"""

    time_period_body = """<?xml version="1.0" encoding="utf-8"?>
                            <soap:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
                              <soap:Body>
                                <DiscoverParameterValues xmlns="http://comscore.com/">
                                  <parameterId>timePeriod</parameterId>
                                     <query xmlns="http://comscore.com/ReportQuery">
                                        <Parameter KeyId="geo" Value="{}" />
                                        <Parameter KeyId="loc" Value="1" />
                                        <Parameter KeyId="timeType" Value="1" />
                                     </query>
                                </DiscoverParameterValues>
                              </soap:Body>
                            </soap:Envelope>"""

    # Region code array, dict
    geos = {
        1: "worldwide",
        2: "canada"
    }

    # Target code array, dict
    targets = {
        158: "male_15_24",
        160: "male_25_34",
        161: "male_35_44",
        162: "male_45_54",
        164: "male_55_plus",
        171: "women_15_24",
        173: "women_25_34",
        174: "women_35_44",
        175: "women_45_54",
        177: "women_55_Plus",
    }

    # Media set type array, dict

    media_sets = {
        1: "ranked",
        2: "expanded"
    }

    # Iterate over each geographical region
    for geo in geos:
        # Get the time periods for the geographical region
        time_periods = get_time_period(url, auth, time_period_body, geo)
        # Iterate over each time period
        for time_period in time_periods:
            # Iterate over each target
            for target in targets:
                # Iterate over each media set
                for media_set in media_sets:
                    body = submit_body.format(geo, time_period, target, media_set)
                    # Get the names for each item from the dict for pretty file naming
                    get_report(url, auth, body, geos[geo], time_periods[time_period], targets[target],
                               media_sets[media_set])


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
