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

    try:
        pd_reader = pd.read_csv(parsed_csv, header=0)
        df = pd.DataFrame(pd_reader)

        df.insert(13, "TITLETYPE", 'NULL', allow_duplicates=True)
        df.insert(14, "FRAMERATE", 'NULL', allow_duplicates=True)
        df.insert(15, "CODEC", 'NULL', allow_duplicates=True)
        df.insert(16, "V_WIDTH", 'NULL', allow_duplicates=True)
        df.insert(17, "V_HEIGHT", 'NULL', allow_duplicates=True)
        df.insert(18, "TRAFFIC_CODE", 'NULL', allow_duplicates=True)
        df.insert(19, "DURATION_MS", 'NULL', allow_duplicates=True)
        df.insert(20, "XML_CREATED", 0, allow_duplicates=True)

        df.to_csv(clean_csv)

        for index, row in df.iterrows():

            name = str(row['NAME']).upper()
            print(str(index) + "    " + name)

            if(pd.isnull(row['METAXML'])):
                df.at[index, 'METAXML'] = 'NULL'

            else:
                r_metaxml = row['METAXML']
                df.at[index, 'METAXML'] = clean_metaxml(r_metaxml, name)

            traffic_code = get_traffic_code(name)
            df.at[index, 'TRAFFIC_CODE'] = traffic_code

            if row['_merge'] is not "both":
                df.drop(df.index)

            video_check = re.search(r'([_]VM)|([_]EM)|([_]UHD)', name)

            archive_check = re.search(r'([_]AVP)|([_]PPRO)|([_]FCP)|([_]PTS)|([_]AVP)|([_]GRFX)|([_]GFX)|([_]WAV)', name)

            if (
                video_check is not None
                and archive_check is None
                and metaxml is not 'NULL'
                ):
                df.at[index, 'TITLETYPE'] = 'video'
                content_type = re.sub('_', '', video_check.group(0))
                mediainfo = get_mediainfo(row)

            elif (
                video_check is not None
                and archive_check is None
                and metaxml == 'NULL'
                ):
                df.at[index, 'TITLETYPE'] = 'video'
                content_type = re.sub('_', '', video_check.group(0))

            elif video_check is None and archive_check is not None:
                df.at[index, 'TITLETYPE'] = 'archive'
                content_type = re.sub('_', '', archive_check.group(0))
                mediainfo = ['NULL', 'NULL', 'NULL', 'NULL', 'NULL', 'NULL',]

            elif video_check is not None and archive_check is not None:
                df.at[index, 'TITLETYPE'] = 'archive'
                content_type = re.sub('_', '', archive_check.group(0))
                mediainfo = ['NULL', 'NULL', 'NULL', 'NULL', 'NULL', 'NULL',]

            else:
                df.at[index, 'TITLETYPE'] = 'NULL'
                clean_2_msg = f"TITLETYPE for {name} is NULL. "
                logger.info(clean_2_msg)
                content_type = 'NULL'
                mediainfo = ['NULL', 'NULL', 'NULL', 'NULL', 'NULL', 'NULL',]

            df.at[index, 'CONTENT_TYPE'] = content_type
            df.at[index, 'FRAMERATE'] = mediainfo[0]
            df.at[index, 'CODEC'] = mediainfo[1]
            df.at[index, 'V_WIDTH'] = mediainfo[2]
            df.at[index, 'V_HEIGHT'] = mediainfo[3]
            df.at[index, 'DURATION_MS'] = mediainfo[4]

        df.to_csv(clean_csv)

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

    if row['METAXML'] != 0 or row['METAXML'] is not 'NULL':

        try:
            tree = ET.ElementTree(ET.fromstring(row['METAXML']))
            root = tree.getroot()

            codec = root.find('VideoTrack/Video/Format').text
            framerate = root.find('VideoTrack/Video/AverageFrameRate').text
            v_width = root.find('VideoTrack/Video/Width').text
            v_height = root.find('VideoTrack/Video/Height').text
            duration = root.find('DurationInMs').text

            if codec == "AVC" and int(v_width) < 720:
                codec = "NULL"
                if (int(row['FILESIZE']) > 30000000000 and
                    int(row['FILESIZE']) < 200000000000):
                    v_width = '1920'
                    v_height = '1080'
                    est_msg = f"{row['GUID']} - {row['NAME']} - Estimating file is HD - 1920x1080."
                    logger.info(est_msg)
                else:
                    v_width = "NULL"
                    v_height = "NULL"

            if v_height == '1062' and v_width == '1888':
                v_width = '1920'
                v_height = '1080'

            else:
                pass

            mediainfo = [framerate, codec, v_width, v_height, duration]

        except Exception as e:
            mediainfo_err_msg = f"\
            \n\
            Exception raised on get_mediainfo.\n\
            Setting mediainfo values to 'NULL'\n\
            GUID: {row['GUID']}\n\
            NAME: {row['NAME']}\n\
            \n"

            print("")
            print(row['METAXML'])
            print("")

            logger.exception(mediainfo_err_msg)

            framerate='NULL'
            codec='NULL'
            v_width='NULL'
            v_height='NULL'
            duration='NULL'
            mediainfo = [framerate, codec, v_width, v_height, duration]

    else:
        framerate='NULL'
        codec='NULL'
        v_width='NULL'
        v_height='NULL'
        duration='NULL'

    return mediainfo

def get_traffic_code(name):
    """
    Validate the the traffic code begins with a 0, and contains the correct number of characters.
    """

    name = str(name)

    if name.startswith('0') is not True:

        x = re.search(r"_?[0-9]{6}(_|-)?", name)
        # y = re.search(r"[0-9]{6}", name)

        if x is not None:
            traffic_code = x.group(0)
        else:
            err_msg = f"Incompatible file ID - {str(name)}. traffic_code set to NULL"
            logger.error(err_msg)
            traffic_code = 'NULL'

    else:
        traffic_code = "=\"" + name[:6] + "\""

    return traffic_code


def clean_metaxml(r_metaxml, name):

    xml_search = re.search(r"[<FileName>].*&.*[</FileName>]", r_metaxml)

    if xml_search is not None:
        bad_xml = xml_search.group(0)
        good_xml = bad_xml.replace("&", "and")
        metaxml = good_xml

        clean_xml_msg = f"metaxml for {name} was modified to remove & characters."
        logger.info(clean_xml_msg)

    else:
        metaxml = r_metaxml

    return metaxml


if __name__ == '__main__':
    db_clean()
