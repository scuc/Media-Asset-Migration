 #! /usr/bin/env python3

import logging
import os

import config as cfg

import database as db

from xml.dom import minidom

logger = logging.getLogger(__name__)


def create_xml(xml_total):
    """
    Use the merged, parsed, cleaned, DB data to generate an XML used for file check in on the MAM.
    """

    config = cfg.get_config()
    rootpath = config['paths']['rootpath']
    xml_checkin = config['paths']['xml_checkin_path']
    os.chdir(rootpath)

    xml_1_msg = f"START GORILLA-DIVA XML CREATION"
    logger.info(xml_1_msg)

    try:
        conn = db.connect()
        cur = conn.cursor()
        sql = '''SELECT * FROM assets WHERE xml_created = 0'''

        xml_count = 0

        for row in cur.execute(sql).fetchall():
            ROWID = row[0]
            GUID = row[1]
            NAME = row[2]
            FILESIZE = row[3]
            DATATAPEID = row[4]
            OBJECTNM = row[5]
            CONTENTLENGTH = row[6]
            SOURCECREATEDT = row[7]
            CREATEDT = row[8]
            LASTMDYDT = row[9]
            TIMECODEIN = row[10]
            TIMECODEOUT = row[11]
            ONAIRID = row[12]
            RURI = row[13]
            TITLETYPE = row[14]
            FRAMERATE = row[15]
            CODEC = row[16]
            V_WIDTH = row[17]
            V_HEIGHT = row[18]
            TRAFFIC_CODE = row[19]
            DURATION_MS = row[20]
            XML_CREATED = row[21]
            PROXY_COPIED = row[22]
            CONTENT_TYPE = row[23]
            METAXML = row[24]
            OC_COMPONENT_NAME = row[31]

            if (int(xml_total) > xml_count
                and DATATAPEID != 'unallocated'
                and DATATAPEID != 'NULL'
                and OC_COMPONENT_NAME != 'NULL'):
                guid = GUID
                name = NAME
                datatapeid = DATATAPEID
                timecodein = TIMECODEIN
                folderpath = "T://DaletStorage/Video_Watch_Folder" + str(OC_COMPONENT_NAME)
                traffic_code = str(TRAFFIC_CODE).strip("=\"")
                title_type = TITLETYPE
                framerate = FRAMERATE
                codec = CODEC
                v_width = V_WIDTH
                v_height = V_HEIGHT
                duration = DURATION_MS
                content_type = CONTENT_TYPE

                conn.close()
                os.chdir(xml_checkin)
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
                    <mediaFormatId>100002</mediaFormatId>\
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

                os.chdir(rootpath)
                update = db.update_column('assets', 'xml_created', 1, ROWID)
                xmlcreate_msg = (f"\n\
                                RowID: {str(ROWID)}\n\
                                xml_count: {xml_count}\n\
                                xml_doc:  {str(xml_doc)}\n")
                logger.info(xmlcreate_msg)
                xml_count += 1
            else:
                xml_pass_msg = f"XML Creation skipped on {ROWID} for asset {GUID}. DATETAPEID = {DATATAPEID}"
                logger.info(xml_pass_msg)
                pass

        xml_2_msg = f"GORILLA-DIVA XML CREATION COMPLETED"
        logger.info(xml_2_msg)

    except Exception as e:
        xml_excp_msg = f"\n\
        Exception raised on the XML Creation.\n\
        ROWID = {ROWID}\n\
        Error Message:  {str(e)} \n\
        XML Count: {xml_count}\n\
        "

        logger.exception(xml_excp_msg)


if __name__ == '__main__':
    create_xml(10000)

