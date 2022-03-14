#! /usr/bin/env python3

import logging
import os
import re

import config as cfg
import pandas as pd

import database as db
import get_mediainfo as gmi

logger = logging.getLogger(__name__)


def csv_clean(date):
    """
    Cleaning the merged data follows mulitple steps: 
        - put the merged CSV into a pandas dataframe
        - insert new fields into the dataframe (columns 13 to 23)
        - iterate over all rows in the dataframe
        - clean each filename to remove illegal character: &
        - parse the Traffic Code from the cleaned name and validate the value
        - parse and clean the METAXML field if it is not NULL
        - perform regex search against the clean filename to determine if the object 
            video or a ZIP archive 
        - assign a "content type" based on results of the regex searches
        - if file has mediainfo - assign values for these new fields based on media info
        - if the file has NO mediainfo - assign mediainfo values to NULL and create filename based
            on the cleaned name + the object creation date
        - drop the METAXML field from the dataframe, export a new CSV,
            then take cleaned dataframe data and create a DB
            with tablename "assets"
    """

    config = cfg.get_config()

    rootpath = config['paths']['rootpath']
    dbpath = config['paths']['dbpath']
    csvpath = config['paths']['csvpath']

    os.chdir(csvpath)

    parsed_csv = date + "_" + "gor_diva_merged_parsed.csv"
    clean_csv = date + "_" + "gor_diva_merged_cleaned.csv"

    clean_1_msg = f"START GORILLA-DIVA DB CLEAN"
    logger.info(clean_1_msg)

    try:
        pd_reader = pd.read_csv(parsed_csv, header=0)
        df = pd.DataFrame(pd_reader)
        df.index.name = 'ROWID'
        df = df.astype({"METAXML": str}) #set the field to type str

        df.insert(13, "TITLETYPE", 'NULL', allow_duplicates=True)
        df.insert(14, "FRAMERATE", 'NULL', allow_duplicates=True)
        df.insert(15, "CODEC", 'NULL', allow_duplicates=True)
        df.insert(16, "V_WIDTH", 'NULL', allow_duplicates=True)
        df.insert(17, "V_HEIGHT", 'NULL', allow_duplicates=True)
        df.insert(18, "TRAFFIC_CODE", 'NULL', allow_duplicates=True)
        df.insert(19, "DURATION_MS", 'NULL', allow_duplicates=True)
        df.insert(20, "XML_CREATED", 0, allow_duplicates=True)
        df.insert(21, "PROXY_COPIED", 0, allow_duplicates=True)
        df.insert(22, "CONTENT_TYPE", 'NULL', allow_duplicates=True)
        df.insert(23, "FILENAME", 'NULL', allow_duplicates=True)

        df.to_csv(clean_csv)

        for index, row in df.iterrows():

            name = str(row['NAME']).upper()
            cleaned_name = clean_name(name)
            df.at[index, 'NAME'] = cleaned_name

            name_clean_msg = f"Index: {str(index)}  cleaned filename: {cleaned_name}"
            logger.info(name_clean_msg)

            traffic_code = get_traffic_code(cleaned_name)
            df.at[index, 'TRAFFIC_CODE'] = traffic_code

            df_row = df.loc[index]

            if row['_merge'] != "both":
                df.drop(index=index, inplace=True)
                continue
            else:
                pass

            if pd.isnull(df_row['METAXML']) is not True:
                l_metaxml = df_row['METAXML']
                r_metaxml = r'{}'.format(l_metaxml)
                metaxml = clean_metaxml(r_metaxml, cleaned_name)
                df.at[index, 'METAXML'] = metaxml
            else:
                df.at[index, 'METAXML'] = 'NULL'
                metaxml = df.at[index, 'METAXML']

            video_check_1 = re.search(
                r'((?<![0-9]|[A-Z])|(?<=(-|_)))(VM)(?=(-|_|[1-5])?)(?![A-Z])', cleaned_name)
            video_check_2 = re.search(
                r'((?<![0-9]|[A-Z])|(?<=(-|_)))(EM)(?=(-|_|[1-5])?)(?![A-Z])', cleaned_name)
            video_check_3 = re.search(
                r'((?<![0-9]|[A-Z])|(?<=(-|_)))(UHD)(?=(-|_|[1-5])?)(?![A-Z])', cleaned_name)
            video_check_4 = re.search(
                r'((?<![0-9]|[A-Z])|(?<=(-|_)))(XDCAM)(?=(-|_|[1-5]|HD)?)', cleaned_name)

            vcheck_list = []

            if (video_check_1, video_check_2, video_check_3, video_check_4) != (None, None, None, None):
                if video_check_1 is not None:
                    vcheck1 = video_check_1.group(0)
                    vcheck_list.append(vcheck1)

                if video_check_2 is not None:
                    vcheck2 = video_check_2.group(0)
                    vcheck_list.append(vcheck2)

                if video_check_3 is not None:
                    vcheck3 = video_check_3.group(0)
                    vcheck_list.append(vcheck3)

                if video_check_4 is not None:
                    vcheck4 = video_check_4.group(0)
                    vcheck_list.append(vcheck4)
                content_type_v = ",".join(vcheck_list)

            else:
                content_type_v = None
                
            archive_check = re.search(r'((?<![0-9]|[A-Z])|(?<=(-|_)))(AVP|PPRO|FCP|PTS|AVP|GRFX|GFX|WAV|WAVS|SPLITS)(?=(-|_)?)(?![0-9]|[A-Z])', cleaned_name)

            if archive_check is not None: 
                if archive_check.group(0) == 'SPLITS':
                    content_type_a = "WAV"
                elif archive_check.group(0) == 'WAVS':
                    content_type_a = "WAV"
                elif archive_check.group(0) == 'GFX':
                    content_type_a = "GRFX"
                else:
                    content_type_a = archive_check.group(0)
            else: 
                content_type_a = None

            if (content_type_v is not None
                and archive_check is None):
                df.at[index, 'TITLETYPE'] = 'video'
                mediainfo = gmi.get_mediainfo(df_row, metaxml)

                print("")
                print("MEDIA-INFO:   " + str(mediainfo))
                print("")

                df.at[index, 'CONTENT_TYPE'] = content_type_v
                df.at[index, 'FRAMERATE'] = mediainfo[0]
                df.at[index, 'CODEC'] = mediainfo[1]
                df.at[index, 'V_WIDTH'] = mediainfo[2]
                df.at[index, 'V_HEIGHT'] = mediainfo[3]
                df.at[index, 'DURATION_MS'] = mediainfo[4]
                df.at[index, 'FILENAME'] = mediainfo[5]

            elif (content_type_v is None
                  and archive_check is not None):
                title_type = get_title_type(content_type_a)
                date = df.at[index, 'SOURCECREATEDT']
                creation_date = format_creation_date(date)
                df.at[index, 'TITLETYPE'] = title_type
                df.at[index, 'CONTENT_TYPE'] = content_type_a
                df.at[index, 'PROXY_COPIED'] = 3
                df.at[index, 'FILENAME'] = f"{cleaned_name}_{creation_date}.zip" 
                mediainfo = ['NULL', 'NULL', 'NULL', 'NULL', 'NULL', 'NULL', ]

            elif (content_type_v is not None
                  and archive_check is not None):
                title_type = get_title_type(content_type_a)
                date = df.at[index, 'SOURCECREATEDT']
                creation_date = format_creation_date(date)
                df.at[index, 'TITLETYPE'] = title_type
                df.at[index, 'CONTENT_TYPE'] = content_type_a
                df.at[index, 'PROXY_COPIED'] = 3
                df.at[index, 'FILENAME'] = f"{cleaned_name}_{creation_date}.zip" 
                mediainfo = ['NULL', 'NULL', 'NULL', 'NULL', 'NULL', 'NULL', ]

            else:
                df.at[index, 'TITLETYPE'] = 'NULL'
                clean_2_msg = f"TITLETYPE for {name} is NULL. "
                logger.info(clean_2_msg)
                df.at[index, 'CONTENT_TYPE'] = 'NULL'
                mediainfo = ['NULL', 'NULL', 'NULL', 'NULL', 'NULL', 'NULL', ]

        df.at[index, 'FILENAME'] = mediainfo[5]
        df.drop("METAXML", axis=1, inplace=True)
        df.to_csv(clean_csv)
        os.chdir(dbpath)

        conn = db.connect()
        tablename = 'assets'
        db.create_table('database.db', tablename, df)

        clean_3_msg = f"GORILLA-DIVA DB CLEAN COMPLETE, NEW DB TABLE CREATED"
        logger.info(clean_3_msg)

        os.chdir(rootpath)
        return clean_csv, tablename

    except Exception as e:
        db_clean_excp_msg = f"\n\
        Exception raised on the Gor-Diva DB Clean.\n\
        Error Message:  {str(e)} \n\
        Index Count: {index}\n\
        "
        logger.exception(db_clean_excp_msg)


def get_title_type(content_type_a): 
    """
    Check content_type for specific tags, and apply a title_type based on those tags. 
    """
    if content_type_a == "WAV":
        title_type = "audio"
    if content_type_a == "PTS":
        title_type = "AdobeAuditionSession"
    if content_type_a in ("FCP", "AVP", "PPRO"):
        title_type = "AdobePremiereProject"
    if content_type_a == "GRFX":
        title_type = "archive"
    else: 
        title_type == "archive"
    
    return title_type


def get_traffic_code(cleaned_name):
    """
    Validate the the traffic code begins with a 0, and contains the correct number of characters.
    """

    name = str(cleaned_name)

    if name.startswith('0') is not True:
        code_search = re.search(
            r"((?<![0-9]|[A-Z])|(?<=(-|_)))0[0-9]{5}(?=(_|-))", name)

        if code_search is not None:
            traffic_match = code_search.group(0)
            # traffic_code_r = re.sub(r'(_|-)', '', traffic_match)
            traffic_code = "=\"" + traffic_match + "\""
        else:
            err_msg = f"Incompatible file ID - {str(name)}. traffic_code set to NULL"
            logger.error(err_msg)
            traffic_code = 'NULL'
    else:
        traffic_code = "=\"" + name[:6] + "\""

    return traffic_code


def format_creation_date(date):
    """
    Remove non-integer characters from the date string.
    """
    creation_date = (date.translate({ord(i): None for i in '- :'}))
    return creation_date


def clean_metaxml(r_metaxml, name):
    """
    Replace '&'  and '\\' characters in the metaxml field. 
    """

    xml_search = re.search(r"[<FileName>].*&.*[</FileName>]", r_metaxml)

    if xml_search is not None:
        bad_xml = xml_search.group(0)
        good_xml = bad_xml.replace("&", "and")
        metaxml = good_xml.replace("\\", "/")
        clean_xml_msg = f"metaxml for {name} was modified to remove '&' characters."
        logger.info(clean_xml_msg)
    else:
        metaxml = r_metaxml.replace("\\", "/")

    return metaxml


def clean_name(name):
    """
    Replace '&' character with 'and' in the filename field. 
    """

    name_search = re.search(r".*&.*", name)
    name_msg = f"Filename for cleanup:  {name}"
    logger.info(name_msg)

    if (name_search is not None and
        len(name_search.group(0)) != 0):

        bad_name = name_search.group(0)
        good_name = bad_name.replace("&", "and")
        cleaned_name = good_name
        clean_name_msg = f"metaxml for {name} was modified to remove '&' characters."
        logger.info(clean_name_msg)
    else:
        cleaned_name = name

    clean_name_msg = f"Filename after cleanup:  {cleaned_name}"
    logger.info(clean_name_msg)

    return cleaned_name


if __name__ == '__main__':
    csv_clean('202003111354')
