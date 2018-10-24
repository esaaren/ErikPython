class KeymeasuresRequestHelper:
    def __init__(self):
        self.url = 'https://api.comscore.com/KeyMeasures.asmx'

        self.auth = 'Basic dHRjX2JtZW5kb25jYTp4UWFScmtiMkRESGFGMm4='

        self.time_period_body = """<?xml version="1.0" encoding="utf-8"?>
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

        self.target_body = """<?xml version="1.0" encoding="utf-8"?>
                                    <soap:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
                                      <soap:Body>
                                        <DiscoverParameterValues xmlns="http://comscore.com/">
                                          <parameterId>target</parameterId>
                                             <query xmlns="http://comscore.com/ReportQuery">
                                                <Parameter KeyId="geo" Value="{}" />
                                                <Parameter KeyId="timeType" Value="1" />
                                                <Parameter KeyId="targetType" Value="0" />
                                                <Parameter KeyId="timePeriod" Value="{}" />
                                             </query>
                                        </DiscoverParameterValues>
                                      </soap:Body>
                                    </soap:Envelope>"""

        self.media_set_body = """<?xml version="1.0" encoding="utf-8"?>
                                <soap:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
                                  <soap:Body>
                                    <DiscoverParameterValues xmlns="http://comscore.com/">
                                      <parameterId>mediaSetType</parameterId>
                                         <query xmlns="http://comscore.com/ReportQuery">
                                            <Parameter KeyId="geo" Value="{}" />
                                            <Parameter KeyId="timePeriod" Value="{}" />
                                            <Parameter KeyId="timeType" Value="1" />
                                         </query>
                                    </DiscoverParameterValues>
                                  </soap:Body>
                                </soap:Envelope>"""

        self.submit_body = """<?xml version="1.0" encoding="utf-8"?>
                                    <soap:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
                                        <soap:Body>
                                        <SubmitReport xmlns="http://comscore.com/">
                                            <query xmlns="http://comscore.com/ReportQuery">
                                                <Parameter KeyId="geo" Value="{}" />
                                                <Parameter KeyId="loc" Value="0" />
                                                <Parameter KeyId="timeType" Value="1" />
                                                <Parameter KeyId="timePeriod" Value="{}" />
                                                <Parameter KeyId="targetType" Value="0" />
                                                <Parameter KeyId="target" Value="{}" />
                                                <Parameter KeyId="measure" Value="1" />
                                                <Parameter KeyId="measure" Value="9" />
                                                <Parameter KeyId="measure" Value="10" />
                                                <Parameter KeyId="measure" Value="70" />
                                                <Parameter KeyId="measure" Value="71" />
                                                <Parameter KeyId="measure" Value="7" />
                                                <Parameter KeyId="measure" Value="2" />
                                                <Parameter KeyId="measure" Value="14" />
                                                <Parameter KeyId="measure" Value="3" />
                                                <Parameter KeyId="measure" Value="16" />
                                                <Parameter KeyId="measure" Value="15" />
                                                <Parameter KeyId="measure" Value="8" />
                                                <Parameter KeyId="measure" Value="6" />
                                                <Parameter KeyId="measure" Value="5" />
                                                <Parameter KeyId="measure" Value="11" />
                                                <Parameter KeyId="measure" Value="12" />
                                                <Parameter KeyId="measure" Value="143" />
                                                <Parameter KeyId="measure" Value="144" />
                                                <Parameter KeyId="measure" Value="145" />
                                                <Parameter KeyId="measure" Value="146" />
                                                <Parameter KeyId="measure" Value="274" />
                                                <Parameter KeyId="mediaSet" Value="4962873" />
                                                <Parameter KeyId="mediaSetType" Value="{}" />
                                            </query>
                                        </SubmitReport>
                                        </soap:Body>
                                    </soap:Envelope>"""