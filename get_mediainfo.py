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

    if df_row['METAXML'] != 'nan':

        try:
            parser = ET.XMLParser(encoding="utf-8")
            tree = ET.ElementTree(ET.fromstring(metaxml, parser=parser))
            root = tree.getroot()

            codec = root.find('VideoTrack/Video/Format').text
            framerate = root.find('VideoTrack/Video/AverageFrameRate').text
            v_width = root.find('VideoTrack/Video/Width').text
            v_height = root.find('VideoTrack/Video/Height').text
            duration = root.find('DurationInMs').text

            
            codec_match = re.search(r'(UHD|XAVC(?=[-UHD]?|-|_))|PRORES(?=[HQ]?|-|_)', df_row['NAME'])
            
            if codec_match is not None: 
                codec = codec_match.group(0)
            else:
                codec = 'NULL'
                
            if v_height == '1062' and v_width == '1888':
                v_width = '1920'
                v_height = '1080'

            v_width, v_height = est_resolution(df_row, codec)

            mediainfo = [framerate, codec, v_width, v_height, duration]

        except Exception as e:
            mediainfo_err_msg = f"\
            \n\
            Exception raised on 1st block of get_mediainfo.\n\
            GUID:  {df_row['GUID']}\n\
            NAME:  {df_row['NAME']}\n\
            ERROR:  {str(e)} \n\
            \n"

            logger.exception(mediainfo_err_msg)
   
    else:
        try:
            codec_match = re.search(
                r'(UHD|XAVC(?=[-UHD]?|-|_))|PRORES(?=[HQ]?|-|_)|XDCAM(?=[HD]?|-|_)', df_row['NAME'])
            framerate_match = re.search(
                r'(?<![0-9]|[A-Z])[25][359][\.]?([46789]{1,3}(?=[IP]?))?|(?<=(-|_))(NTSC|PAL)(?=(-|_)?)', df_row['NAME'])
            duration_match=int(df_row['CONTENTLENGTH']) * 1000
            v_width, v_height = est_resolution(df_row, codec_match)

            if framerate_match is not None:
                framerate_value=framerate_match.group(0)
                if framerate_value in ['2398','23976','2997','5994']:
                    framerate = framerate_value[0:2] + "." + framerate_value[2:]
                elif framerate_value == 'NTSC':
                    framerate = 29.97
                elif framerate_value == 'PAL':
                    framerate = 25
                else: 
                    framerate = framerate_value
                est_msg = f"{df_row['GUID']} - {df_row['NAME']} - Framerate {framerate} value based on filename."
                logger.info(est_msg)
            else:
                framerate='NULL'

            if duration_match != 0:
                duration=duration_match
            else:
                duration=0

            if codec_match is not None:
                codec_value = codec_match.group(0)
                if str(codec_value) == "XDCAM":
                    codec = "MPEG Video"
                    v_width = '1920'
                    v_height = '1080'
                    est_msg = f"{df_row['GUID']} - {df_row['NAME']} - filesize: {df_row['FILESIZE']} - Estimating file is HD - 1920x1080."
                    logger.info(est_msg)
                else:
                    codec = codec_value
            else:
                codec = 'NULL'

            mediainfo = [framerate, codec, v_width, v_height, duration]
            
        except Exception as e:
            mediainfo_err_msg=f"\
            \n\
            Exception raised on 2nd block of get_mediainfo.\n\
            GUID:  {df_row['GUID']}\n\
            NAME:  {df_row['NAME']}\n\
            ERROR:  {str(e)} \n\
            \n"

            logger.exception(mediainfo_err_msg)
    
    return mediainfo


def est_resolution(df_row, codec):
    """
    Estimate the resolution based on filesize.
    """

    if (int(df_row['FILESIZE']) > 18000000000 and
        int(df_row['FILESIZE']) < 200000000000
        and codec is not 'XAVC' 
        and codec is not 'UHD'
        and df_row['CONTENTLENGTH'] != 0):
        v_width='1920'
        v_height='1080'
        est_msg=f"{df_row['GUID']} - {df_row['NAME']} - filesize: {df_row['FILESIZE']} - Estimating file is HD - 1920x1080."
        logger.info(est_msg)
    elif codec is 'XAVC' or codec is 'UHD':
        v_width='3840'
        v_height='2160'
        est_msg=f"{df_row['GUID']} - {df_row['NAME']} - Estimating file is UHD - 3840x2160."
        logger.info(est_msg)
    else:
        v_width="NULL"
        v_height="NULL"
        est_msg = f"{df_row['GUID']} - {df_row['NAME']} - Cannot determine v_width or v_height, setting to Null."
        logger.info(est_msg)

    return v_width, v_height


if __name__ == '__main__':
    get_mediainfo()
