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
            filename = root.find('FileName').text
                
            if v_height == '1062' and v_width == '1888':
                v_width = '1920'
                v_height = '1080'

            if (v_height == '360' 
                and v_width == '640'
                and codec == 'AVC'): 
                v_width = '1920'
                v_height = '1080'
                codec = 'ProRes'

            if framerate not in ['23.98', '25', '29.97', '59.94']:
                framerate = framerate[0:2]
                if framerate == '23': 
                    framerate = '23.98'
                elif framerate == '25': 
                    framerate = '25'
                elif framerate == '29':
                    framerate = '29.97'
                elif framerate == '59':
                    framerate = '59.94'
                else:
                    framerate = get_framerate(df_row)

            mediainfo = [framerate, codec, v_width, v_height, duration, filename]

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
            framerate = get_framerate(df_row)
            codec, codec_value = get_codec(df_row)
            v_width, v_height = est_resolution(df_row, codec_value)

            duration_match = int(df_row['CONTENTLENGTH']) * 1000

            if duration_match != 0:
                duration=duration_match
            else:
                duration=0

            filename = df_row['NAME']

            mediainfo = [framerate, codec, v_width, v_height, duration, filename]
            
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


def get_codec(df_row):
    """
    Match the codec of a file using the info in the filename. 
    """
    codec_match = re.search(r'(((?<![A-Z])|(?<=(-|_)))(UHD|XAVC|UHD|PRORES|XDCAM|DNX)(?=(-|_|HQ|HD)?))', df_row['NAME'])
    
    if codec_match is not None:
        codec_value = codec_match.group(0)
        if str(codec_value) == "XDCAM":
            codec = "MPEG Video"
        elif str(codec_value) == "PRORES":
            codec = "PRORES"
        elif str(codec_value) == "DNXHD":
            codec = "VC-3"
        elif str(codec_value) == "UHD":
            codec = "XAVC"
        elif str(codec_value) == "XAVC":
            codec = "XAVC"
        else:
            codec = codec_value

    else:
        codec = 'NULL'
        codec_value = 'NULL'

    est_msg = f'{df_row["GUID"]} - {df_row["NAME"]} - Estimating Codec based on filename: {codec}'
    logger.info(est_msg)

    return codec, codec_value


def get_framerate(df_row):
    """
    Match the framerate of a file using the info in the filename.
    """
    framerate_match = re.search(
        r'(?<![0-9]|[A-Z])(23|25|29|59)\.?((98|976|97|94)(?=[IP]?))?|(?<=(-|_))(NTSC|PAL)(?=(-|_)?)|(?<=(-|_))(24P|720P)(?=(-|_)?)', df_row['NAME'][6:])
    if framerate_match is not None:
        framerate_value = framerate_match.group(0)
        if framerate_value in ['2398', '23976', '2997', '5994']:
            framerate = framerate_value[0:2] + \
                "." + framerate_value[2:]
        elif framerate_value == 'NTSC':
            framerate = '29.97'
        elif framerate_value == 'PAL':
            framerate = '25'
        elif framerate_value == '24P':
            framerate = '24'
        elif framerate_value == '720P':
            framerate = '59.94'
        else:
            framerate = framerate_value
    
        est_msg = f"{df_row['GUID']} - {df_row['NAME']} - Framerate {framerate} value based on filename."
        logger.info(est_msg)

    else:
        framerate = 'NULL'

    return framerate


def est_resolution(df_row, codec_value):
    """
    Estimate the resolution based on filesize and codec.
    """

    if (int(df_row['FILESIZE']) > 18000000000 and
        int(df_row['FILESIZE']) < 200000000000
        and codec_value !='XAVC' 
        and codec_value != 'UHD'
        and df_row['CONTENTLENGTH'] != 0):
        v_width='1920'
        v_height='1080'
        est_msg=f"{df_row['GUID']} - {df_row['NAME']} - filesize: {df_row['FILESIZE']} - Estimating file is HD: 1920x1080."
        logger.info(est_msg)
    elif (codec_value == 'XAVC' 
         or codec_value == 'UHD'
         and int(df_row['FILESIZE']) > 18000000000):
        v_width='3840'
        v_height='2160'
        est_msg = f"{df_row['GUID']} - {df_row['NAME']} - filesize: {df_row['FILESIZE']} - Estimating file is UHD:3840x2160."
        logger.info(est_msg)
    elif (v_height == '360' 
         and v_width == '640'
         and codec == 'ProRes'): 
        v_width = '1920'
        v_height='1080'
        est_msg = f"{df_row['GUID']} - {df_row['NAME']} - filesize: {df_row['FILESIZE']} - Estimating file is HD:1920x1080."
        logger.info(est_msg)
    else:
        v_width="NULL"
        v_height="NULL"
        est_msg = f"{df_row['GUID']} - {df_row['NAME']} - Cannot determine v_width or v_height, setting to Null."
        logger.info(est_msg)

    return v_width, v_height
    

if __name__ == '__main__':
    get_mediainfo()
