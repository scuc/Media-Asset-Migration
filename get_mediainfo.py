#! /usr/bin/env python3

import logging
import os
import re

import xml.etree.ElementTree as ET

logger = logging.getLogger(__name__)


def get_mediainfo(df_row, metaxml):
    """
    Extract mediainfo from the metaxml field of the DB.
    """

    if df_row['METAXML'] is not 'NULL':

        try:
            parser = ET.XMLParser(encoding="utf-8")
            tree = ET.ElementTree(ET.fromstring(metaxml, parser=parser))
            root = tree.getroot()

            codec = root.find('VideoTrack/Video/Format').text
            framerate = root.find('VideoTrack/Video/AverageFrameRate').text
            v_width = root.find('VideoTrack/Video/Width').text
            v_height = root.find('VideoTrack/Video/Height').text
            duration = root.find('DurationInMs').text

            if codec == "AVC" and int(v_width) < 720:
                codec = "NULL"
                v_width, v_height = est_resolution(df_row)

            if v_height == '1062' and v_width == '1888':
                v_width = '1920'
                v_height = '1080'
            else:
                pass

            mediainfo = [framerate, codec, v_width, v_height, duration]

        except Exception as e:
            mediainfo_err_msg = f"\
            \n\
            Exception raised on 1st block of get_mediainfo.\n\
            GUID:  {df_row['GUID']}\n\
            NAME:  {df_row['NAME']}\n\
            ERROR:  str(e) \n\
            \n"

            logger.exception(mediainfo_err_msg)
   
    else:
        try:
            codec_match = re.search(r'PRORES([HQ]?|-|_)', df_row['NAME'])
            framerate_match = re.search(r'(29(.97)?|23(.98)?|23(.976)?|25|59(.94)?)', df_row['NAME'])
            duration_match=int(df_row['CONTENT_LENGTH']) * 1000
            v_width, v_height = est_resolution(df_row)

            if framerate_match is not None:
                framerate=framerate_match.group(0)
            else:
                framerate='NULL'

            if codec_match is not None:
                codec=codec_match.group(0)
            else:
                codec='NULL'

            if duration_match != 0:
                duration=duration_match
            else:
                duration='NULL'

            mediainfo = [framerate, codec, v_width, v_height, duration]
            
        except Exception as e:
            mediainfo_err_msg=f"\
            \n\
            Exception raised on 2nd block of get_mediainfo.\n\
            GUID:  {df_row['GUID']}\n\
            NAME:  {df_row['NAME']}\n\
            ERROR:  str(e) \n\
            \n"

            logger.exception(mediainfo_err_msg)
    
    return mediainfo


def est_resolution(df_row):
    """
    Estimate the resolution based on filesize.
    """

    if (int(df_row['FILESIZE']) > 33000000000 and
        int(df_row['FILESIZE']) < 200000000000):
        v_width='1920'
        v_height='1080'
        est_msg=f"{df_row['GUID']} - {df_row['NAME']} - Estimating file is HD - 1920x1080."
        logger.info(est_msg)
    else:
        v_width="NULL"
        v_height="NULL"

    return v_width, v_height


if __name__ == '__main__':
    get_mediainfo()