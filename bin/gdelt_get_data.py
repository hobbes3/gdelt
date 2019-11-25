#!/usr/bin/env python
# hobbes3

import logging
import json
import re
import csv
import toml
import splunk_rest.splunk_rest as sr
from splunk_rest.splunk_rest import rest_wrapped
from pathlib import Path
from zipfile import ZipFile
from io import BytesIO, TextIOWrapper
from datetime import datetime

def get_master_list(url):
    logger.debug("Getting master list...", extra={"url": url})

    r = s.get(url)

    if r and r.text:
        append_to_master_list(r.text)

def append_to_data_list(text):
    rows = text.splitlines()

    logger.debug("Got rows.", extra={"row_count": len(rows)})

    skipped = 0

    for i, row in enumerate(rows):
        line = row.split()

        if len(line) != 3:
            logger.warning("Row is invalid! It doesn't contain 3 elements.", extra={"url": url, "i": i, "row": row})
        elif "gkg" not in row:
            url = line[2]
            search = re.search(r"gdeltv2\/(\d+)(?:\.translation)?\.(\w+)\.CSV", url)
            gdelt_id = int(search.group(1)) if search else None
            gdelt_type = "event" if search.group(2) == "export" else "mention"

            if gdelt_id in read_id:
                logger.debug("Skipping row because id has been read before.", extra={"url": url, "id": gdelt_id})
                skipped += 1
            else:
                item = {
                    "url": url,
                    "id": gdelt_id,
                    "type": gdelt_type,
                }

                data_list.append(item)

    logger.debug("Skipped rows.", extra={"skipped_count": skipped})

def get_url(item):
    gdelt_id = item["id"]
    gdelt_type = item["type"]
    url = item["url"]
    r = s.get(url)

    if r and r.content:
        logger.debug("Unzipping url...", extra={"url": url})

        with ZipFile(BytesIO(r.content)) as zf:
            for name in zf.namelist():
                with zf.open(name, "r") as csv_file:
                    csv_header = event_header if gdelt_type == "event" else mention_header
                    reader = csv.DictReader(TextIOWrapper(csv_file), delimiter="\t", fieldnames=csv_header)

                    data = ""

                    for i, row in enumerate(reader, 1):
                        row["splunk_rest"] = {
                            "session_id": sr.session_id
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
                            "index": sr.config["gdelt"]["index"],
                            "sourcetype": "gdelt_" + gdelt_type,
                            "source": __file__,
                            "event": row,
                        }

                        data += json.dumps(event)

                    logger.debug("Found rows in url", extra={"url": url, "row_count": i})

                    rr = s.post(sr.config["hec"]["url"], headers=sr.config["hec"]["headers"], data=data)

                    if rr and rr.text and rr.json()["text"] == "Success":
                        logger.debug("Appending id to .gdelt_read_id file.", extra={"id": gdelt_id})
                        if gdelt_id not in read_id:
                            read_id.append(gdelt_id)
                            Path(".gdelt_read_id").write_text(json.dumps(read_id))

@rest_wrapped
def gdelt_data():
    if debug:
        logger.info("Reading sample master list files...")
        append_to_data_list(Path("sample_masterfilelist.txt").read_text())
        append_to_data_list(Path("sample_masterfilelist-translation.txt").read_text())
    else:
        master_urls = [
            "http://data.gdeltproject.org/gdeltv2/masterfilelist.txt",
            "http://data.gdeltproject.org/gdeltv2/masterfilelist-translation.txt",
        ]

        logger.info("Getting both master file lists... (this will take like 30 seconds)")
        sr.multiprocess(get_master_list, master_urls)

    logger.info("Created data list.", extra={"data_list_count": len(data_list)})

    sr.multiprocess(get_url, data_list)

if __name__ == "__main__":
    logger = logging.getLogger("splunk_rest.splunk_rest")
    s = sr.retry_session()

    debug = sr.config["general"]["debug"]

    read_id_file = Path(".gdelt_read_id")
    read_id = json.loads(read_id_file.read_text()) if read_id_file.exists() else []

    data_list = []

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
