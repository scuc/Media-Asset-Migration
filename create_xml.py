#! /usr/bin/env python3

import logging
import os

import config as cfg
import pandas as pd

from xml.dom import minidom

logger = logging.getLogger(__name__)


def create_xml(cleaned_csv, xml_total):
    """
    Use the merged, parsed, cleaned, DB data to generate an XML used for file check in on the MAM.
    """

    config = cfg.get_config()

    rootpath = config['paths']['rootpath']

    clean_csv = os.path.join(rootpath, '_CSV_Exports/', cleaned_csv)

    xml_1_msg = f"START GORILLA-DIVA XML CREATION"
    logger.info(xml_1_msg)

    index = 0

    with open(clean_csv, mode='r', encoding='utf-8-sig') as c_csv:

        try:
            pd_reader = pd.read_csv(c_csv, header=0)
            df = pd.DataFrame(pd_reader)
            df.sort_values(by=['CREATEDT'], ascending=False)

            count = 0

            for index, row in sdf.iterrows():

                if (row['XML_CREATED'] == '0'
                    and count <= xml_total):

                    guid = row['GUID']
                    name = row['NAME']
                    datatapeid = row['DATATAPEID']
                    timecodein = row['TIMECODEIN']
                    folderpath = "T://DaletStorage/Video_Watch_Folder/" + row["OC_COMPONENT_NAME"]
                    traffic_code = row['TRAFFIC_CODE'].strip("=\"")
                    title_type = row['TITLETYPE']
                    framerate = row['FRAMERATE']
                    codec = row['CODEC']
                    v_width = row['V_WIDTH']
                    v_height = row['V_HEIGHT']
                    duration = row['DURATION_MS']
                    content_type = row['CONTENT_TYPE']

                    os.chdir(rootpath + "_xml/")
                    xml_doc = str(guid) + '.xml'

                    with open(xml_doc, mode="w", encoding='utf-8-sig') as xdoc:

                        xml_body = f"\
                        <Titles> \
                        <Title><!-- title type video -->\
                        <key1>{guid}</key1>\
                        <itemcode>{guid}</itemcode>\
                        <title>{name}</title>\
                        <NGC_NGCITitle>{name}</NGC_NGCITitle>\
                        <NGC_DivaTapeID>{datatapeid}</NGC_DivaTapeID>\
                        <NGC_FolderPath>{folderpath}</NGC_FolderPath>\
                        <StartOfMaterial>{timecodein}</StartOfMaterial>\
                        <NGC_NGCITrafficCode>{traffic_code}</NGC_NGCITrafficCode>\
                        <titletype>{title_type}</titletype>\
                        <NGC_ContentType>{content_type}</NGC_ContentType>\
                        <AMFieldFromParsing_FrameRate>{framerate}</AMFieldFromParsing_FrameRate>\
                        <AMFieldFromParsing_Codec>{codec}</AMFieldFromParsing_Codec>\
                        <AMFieldFromParsing_Width>{v_width}</AMFieldFromParsing_Width>\
                        <AMFieldFromParsing_Hight>{v_height}</AMFieldFromParsing_Hight>\
                        <duration>{duration}</duration>\
                        \
                        <MediaInfos>\
                        <MediaInfo>\
                        <mediaFormatId>100099</mediaFormatId>\
                        <mediaStorageName>G_DIVA</mediaStorageName>\
                        <mediaStorageId>161</mediaStorageId>\
                        <mediaFileName>{guid}</mediaFileName>\
                        <mediaProcessStatus>Online</mediaProcessStatus>\
                        </MediaInfo>\
                        </MediaInfos>\
                        </Title>\
                        </Titles>"

                        xmlstr = minidom.parseString(xml_body).toprettyxml(indent="   ")

                        xdoc.write(xmlstr)
                        xdoc.close()

                    df.at[index, 'XML_CREATED'] = "1"
                    count += 1

            os.chdir(rootpath)
            xml_2_msg = f"GORILLA-DIVA XML CREATION COMPLETED"
            logger.info(xml_2_msg)
            print(xml_2_msg)

        except Exception as e:
            xml_excp_msg = f"\n\
            Exception raised on the XML Creation.\n\
            Error Message:  {str(e)} \n\
            Index Count: {index}\n\
            "

            logger.exception(xml_excp_msg)


if __name__ == '__main__':
    create_xml()


# create_xml(cleaned_csv='201908131300_gor_diva_merged_cleaned.csv')
