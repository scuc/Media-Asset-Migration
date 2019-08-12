#! /usr/bin/env python3

import logging
import os
import re

import config as cfg
import pandas as pd
import xml.etree.ElementTree as ET

logger = logging.getLogger(__name__)


def db_clean(date):
    """
    Parse out data from the db, insert into new fields,  and assign values for these fields in each row.
    """

    config = cfg.get_config()

    rootpath = config['paths']['rootpath']

    os.chdir(rootpath + "_CSV_Exports/")

    parsed_csv = date + "_" + "gor_diva_merged_parsed.csv"
    clean_csv = date + "_" + "gor_diva_merged_cleaned.csv"

    clean_1_msg = f"START GORILLA-DIVA DB CLEAN"
    logger.info(clean_1_msg)
    print(clean_1_msg)

    index = 0

    try:
        pd_reader = pd.read_csv(parsed_csv, header=0, nrows=200)
        df = pd.DataFrame(pd_reader)

        df.insert(13, "TITLETYPE", 'NULL', allow_duplicates=True)
        df.insert(14, "FRAMERATE", 'NULL', allow_duplicates=True)
        df.insert(15, "CODEC", 'NULL', allow_duplicates=True)
        df.insert(16, "V_WIDTH", 'NULL', allow_duplicates=True)
        df.insert(17, "V_HEIGHT", 'NULL', allow_duplicates=True)
        df.insert(18, "TRAFFIC_CODE", 'NULL', allow_duplicates=True)
        df.insert(19, "DURATION_MS", 'NULL', allow_duplicates=True)
        df.insert(20, "XML_CREATED", '0', allow_duplicates=True)

        df.to_csv(clean_csv)

        for index, row in df.iterrows():

            name = str(row['NAME']).upper()
            traffic_code = get_traffic_code(name)
            df.at[index, 'TRAFFIC_CODE'] = "=\"" + traffic_code + "\""

            print(str(index) + "    " + name)

            if row['_merge'] is not "both":
                df.drop(df.index)

            video_check = re.search(r'([_]VM)|([_]EM)|([_]UHD)', name)

            archive_check = re.search(r'([_]AVP)|([_]PPRO)|([_]FCP)|([_]PTS)|([_]AVP)|([_]GRFX)|([_]GFX)', name)

            mediainfo = get_mediainfo(row)

            df.at[index, 'FRAMERATE'] = mediainfo[0]
            df.at[index, 'CODEC'] = mediainfo[1]
            df.at[index, 'V_WIDTH'] = mediainfo[2]
            df.at[index, 'V_HEIGHT'] = mediainfo[3]
            df.at[index, 'DURATION_MS'] = mediainfo[4]

            if video_check is not None and archive_check is None:
                df.at[index, 'TITLETYPE'] = 'video'
                version_type = re.sub('_', '', video_check.group(0))

            elif video_check is None and archive_check is not None:
                df.at[index, 'TITLETYPE'] = 'archive'
                version_type = re.sub('_', '', archive_check.group(0))

            else:
                df.at[index, 'TITLETYPE'] = 'NULL'
                clean_2_msg = f"TITLETYPE for {name} is NULL. "
                logger.info(clean_2_msg)

            df.at[index, 'VERSION_TYPE'] = version_type

            df.to_csv(clean_csv)

            index += 1

        clean_3_msg = f"GORILLA-DIVA DB CLEAN COMPLETE"
        logger.info(clean_3_msg)

        os.chdir(rootpath)

        return clean_csv

    except Exception as e:
        db_clean_excp_msg = f"\n\
        Exception raised on the Gor-Diva DB Clean.\n\
        Error Message:  {str(e)} \n\
        Index Count: {index}\n\
        "
        logger.exception(db_clean_excp_msg)

        print(db_clean_excp_msg)


def get_mediainfo(row):
    """
    Extract mediainfo from the metaxml field of the DB.
    """
    tree = ET.ElementTree(ET.fromstring(row['METAXML']))
    root = tree.getroot()

    codec = root.find('VideoTrack/Video/Format').text
    framerate = root.find('VideoTrack/Video/AverageFrameRate').text
    v_width = root.find('VideoTrack/Video/Width').text
    v_height = root.find('VideoTrack/Video/Height').text
    duration = root.find('DurationInMs').text

    if codec == "AVC" and int(v_width) < 720:
        codec = "NULL"
        v_width = "NULL"
        v_height = "NULL"

    if v_height == '1062' and v_width == '1888':
        v_width = '1920'
        v_height = '1080'

    else:
        pass

    mediainfo = [framerate, codec, v_width, v_height, duration]

    return mediainfo


def get_traffic_code(name):
    """
    Validate the the traffic code begins with a 0, and contains the correct number of characters.
    """

    name = str(name)

    if name.startswith('0') is not True:
        err_msg = f"Incompatible file ID - {str(name)}"

        logger.error(err_msg)

        x = re.search(r"_?[0-9]{6}(_|-)?", name)
        # y = re.search(r"[0-9]{6}", name)

        if x is not None:
            traffic_code = x.group(0)
        else:
            traffic_code = 'NULL'

    else:
        traffic_code = name[:6]

    return traffic_code

# if __name__ == '__main__':
#     db_clean()
