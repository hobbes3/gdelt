#!/usr/bin/env python
# hobbes3

import logging
import json
import re
import csv
import toml
import os
import splunk_rest.splunk_rest as sr
from splunk_rest.splunk_rest import splunk_rest, try_response
from pathlib import Path
from zipfile import ZipFile
from io import BytesIO, TextIOWrapper
from datetime import datetime

def get_master_list(url):
    logger.debug("Getting master list...", extra={"url": url})

    r = s.get(url)

    append_to_data_list(r)

@try_response
def append_to_data_list(r, *args, **kwargs):
    rows = r.text.splitlines()

    logger.debug("Got rows.", extra={"row_count": len(rows)})

    skipped = 0

    for i, row in enumerate(rows):
        line = row.split()

        if "gkg" in row:
            logger.debug("Ignoring gkg urls.", extra={"url": url, "row": i})
        elif len(line) != 3:
            logger.warning("Row doesn't contain 3 elements!", extra={"url": url, "row": i, "text": row})
        else:
            url = line[2]
            urls.append(url)

    logger.debug("Skipped rows.", extra={"skipped_count": skipped})

def get_url(url):
    r = s.get(url)

    @try_response
    def send_events(r, *args, **kwargs):
        meta = {
            "url": url,
            "request_id": r.request_id,
        }

        logger.debug("Unzipping url...", extra=meta)

        with ZipFile(BytesIO(r.content)) as zf:
            for name in zf.namelist():
                with zf.open(name, "r") as csv_file:
                    # https://stackoverflow.com/a/52259169/1150923
                    csv_file.seek(0, os.SEEK_END)
                    if csv_file.tell():
                        csv_file.seek(0)
                    else:
                        logger.warning("Ignoring empty file.", extra=meta)
                        return

                    search = re.search(r"gdeltv2\/(\d+)(?:\.translation)?\.(\w+)\.CSV", url)
                    gdelt_id = int(search.group(1))
                    gdelt_type = "event" if search.group(2) == "export" else "mention"

                    csv_header = event_header if gdelt_type == "event" else mention_header
                    reader = csv.DictReader(TextIOWrapper(csv_file), delimiter="\t", fieldnames=csv_header)

                    data = ""

                    i = 1
                    for i, row in enumerate(reader, 1):
                        row["splunk_rest"] = {
                            "session_id": sr.session_id,
                            "request_id": r.request_id,
                        }
                        row["url"] = url
                        row["row"] = i

                        if gdelt_type == "event":
                            row["QuadClassFull"] = event_quad_class[int(row["QuadClass"])-1]
                        elif gdelt_type == "mention":
                            row["MentionTypeFull"] = mention_types[int(row["MentionType"])-1]

                        event = {
                            # id=20191121011500 becomes
                            # 2019-11-21-01:15:00
                            "time": datetime.strptime(str(gdelt_id), "%Y%m%d%H%M%S").timestamp(),
                            "index": index,
                            "sourcetype": "gdelt_" + gdelt_type,
                            "source": __file__,
                            "event": row,
                        }

                        data += json.dumps(event)

                    m = meta.copy()
                    m["row_count"] = i
                    logger.debug("Found rows in url", extra=m)

                    s.post(hec_url, headers=hec_headers, data=data)

    send_events(r)

@splunk_rest
def gdelt_data():
    # https://blog.gdeltproject.org/gdelt-2-0-our-global-world-in-realtime/
    if script_args.sample:
        logger.info("Reading sample master list files...")
        append_to_data_list(Path("sample_masterfilelist.txt").read_text())
        append_to_data_list(Path("sample_masterfilelist-translation.txt").read_text())
    else:
        #master_urls = [
        #    "http://data.gdeltproject.org/gdeltv2/masterfilelist.txt",
        #    "http://data.gdeltproject.org/gdeltv2/masterfilelist-translation.txt",
        #]
        master_urls = [
            "http://data.gdeltproject.org/gdeltv2/lastupdate.txt",
            "http://data.gdeltproject.org/gdeltv2/lastupdate-translation.txt",
        ]

        logger.info("Getting both master file lists...")
        sr.multiprocess(get_master_list, master_urls)

    logger.info("Created data list.", extra={"data_list_count": len(urls)})

    sr.multiprocess(get_url, urls)

if __name__ == "__main__":
    script_args = sr.get_script_args()
    logger = logging.getLogger("splunk_rest.splunk_rest")
    s = sr.retry_session()

    index = "main" if script_args.test else sr.config["gdelt"]["index"]
    hec_url = sr.config["hec"]["url"]
    hec_headers = sr.config["hec"]["headers"]

    urls = []

    # http://data.gdeltproject.org/documentation/GDELT-Event_Codebook-V2.0.pdf
    event_header = [
        "GlobalEventID",
        "Day",
        "MonthYear",
        "Year",
        "FractionDate",
        "Actor1Code",
        "Actor1Name",
        "Actor1CountryCode",
        "Actor1KnownGroupCode",
        "Actor1EthnicCode",
        "Actor1Religion1Code",
        "Actor1Religion2Code",
        "Actor1Type1Code",
        "Actor1Type2Code",
        "Actor1Type3Code",
        "Actor2Code",
        "Actor2Name",
        "Actor2CountryCode",
        "Actor2KnownGroupCode",
        "Actor2EthnicCode",
        "Actor2Religion1Code",
        "Actor2Religion2Code",
        "Actor2Type1Code",
        "Actor2Type2Code",
        "Actor2Type3Code",
        "IsRootEvent",
        "EventCode",
        "EventBaseCode",
        "EventRootCode",
        "QuadClass",
        "GoldsteinScale",
        "NumMentions",
        "NumSources",
        "NumArticles",
        "AvgTone",
        "Actor1Geo_Type",
        "Actor1Geo_Fullname",
        "Actor1Geo_CountryCode",
        "Actor1Geo_ADM1Code",
        "Actor1Geo_ADM2Code",
        "Actor1Geo_Lat",
        "Actor1Geo_Long",
        "Actor1Geo_FeatureID",
        "Actor2Geo_Type",
        "Actor2Geo_Fullname",
        "Actor2Geo_CountryCode",
        "Actor2Geo_ADM1Code",
        "Actor2Geo_ADM2Code",
        "Actor2Geo_Lat",
        "Actor2Geo_Long",
        "Actor2Geo_FeatureID",
        "ActionGeo_Type",
        "ActionGeo_Fullname",
        "ActionGeo_CountryCode",
        "ActionGeo_ADM1Code",
        "ActionGeo_ADM2Code",
        "ActionGeo_Lat",
        "ActionGeo_Long",
        "ActionGeo_FeatureID",
        "DATEADDED",
        "SOURCEURL",
    ]
    event_quad_class = [
        "Verbal Cooperation",
        "Material Cooperation",
        "Verbal Conflict",
        "Material Conflict",
    ]
    mention_header = [
        "GlobalEventID",
        "EventTimeDate",
        "MentionTimeDate",
        "MentionType",
        "MentionSourceName",
        "MentionIdentifier",
        "SentenceID",
        "Actor1CharOffset",
        "Actor2CharOffset",
        "ActionCharOffset",
        "InRawText",
        "Confidence",
        "MentionDocLen",
        "MentionDocTone",
        "MentionDocTranslationInfo",
        "Extras",
    ]
    mention_types = [
        "WEB",
        "CITATIONONLY",
        "CORE",
        "DTIC",
        "JSTOR",
        "NONTEXTUALSOURCE",
    ]

    gdelt_data()
