
#! /usr/bin/env python3

import logging
import os
import re
import shutil

import config as cfg
import database as db
import pandas as pd
import xml.etree.ElementTree as ET

logger = logging.getLogger(__name__)

config = cfg.get_config()
xml_checkin_path = config['paths']['xml_checkin_path']
proxy_storage_path = config['paths']['proxy_storage_path']
rootpath = config['paths']['rootpath']


def get_root(xml_doc):
    """
    Get the root of an xml document for parsing element tree. 
    """
    tree = ET.parse(xml_doc)
    root = tree.getroot()
    return root


def get_guid(f, f_path):
    """
    Get the guid from an element of the xml file when the filename for the xml has the wrong guid.
    """
    xml_fname = f[:-5]
    dst_path = rootpath + "_tmp/" + xml_fname
    xml_doc = shutil.copy2(f_path, dst_path)
    root = get_root(xml_doc)
    guid_elem = root.find('Title/key1')
    guid = guid_elem.text + '.xml_DONE'
    os.remove(xml_doc)
    return guid


def crosscheck_db(tablename):
    """
    Check db rows against the xmls and proxies in the filesystem. Update the DB records as needed. 
    """
    crosscheck_db_msg = f"BEGIN XML AND PROXY DB-CROSSCHECK"
    logger.info(crosscheck_db_msg)
    print(crosscheck_db_msg)

    try: 
        xml_checkedin_list = []
        for file in os.listdir(xml_checkin_path):
            if not file.startswith("."):
                xml_checkedin_list.append(file)
        
        proxy_checkedin_list = []
        for file in os.listdir(proxy_storage_path):
            if not file.startswith("."):
                proxy_checkedin_list.append(file)

        rows = db.fetchall(tablename)
        for row in rows: 
            xmlname = row[1] + ".xml_DONE"
            if row[4] == "NULL":
                pass
            elif xmlname in xml_checkedin_list: 
                db.update_column(tablename, 'XML_CREATED', 1, row[0])
                xml_update_msg = row[1] + "  xml status updated in the db."
                logger.info(xml_update_msg)
            else:
                db.update_column(tablename, 'XML_CREATED', 0, row[0])
            xml_update_msg = row[1] + "  xml status updated in the db."

            proxyname = row[1] + ".mov"
            if proxyname in proxy_checkedin_list:
                db.update_column(tablename, 'PROXY_COPIED', 1, row[0])
                proxy_update_msg = row[1] + "  proxy status updated in the db."
                logger.info(proxy_update_msg)
            else:
                db.update_column(tablename, 'PROXY_COPIED', 0, row[0])

            print(row[1] + "  proxy status updated")

        print("XML AND PROXY DB-CROSSCHECK COMPLETE")
    except Exception as e:
        cc_excp_msg = f"\n\
        Exception raised on the asset db-crosscheck.\n\
        Error Message:  {str(e)} \n"
        logger.exception(cc_excp_msg)


def crosscheck_assets(tablename): 
    """
    Check files in the filesystem against DB rows. Update the DB records as needed. 
    """

    flist = []
    plist = []

    crosscheck_db_msg = f"BEGIN XML AND PROXY FS-CROSSCHECK"
    logger.info(crosscheck_db_msg)
    print(crosscheck_db_msg)

    try:
        for root, dirs, files in os.walk(xml_checkin_path):
            for f in files:
                f_path = os.path.join(root, f)
                if (f.find("test") != -1
                    or f.startswith(".")):
                    pass
                if not f.endswith(".xml_DONE"):
                    pass
                if (re.search(r"\(\d+\)", f) is not None
                      and f.endswith("_DONE")): 
                    guid = get_guid(f, f_path)
                    flist.append(guid)
                else:
                    flist.append(f)
        xmltocheck = len(flist)

        xml_check_msg = f"Total number of XML files to crosscheck against DB:  {xmltocheck}"
        logger.info(xml_check_msg)

        xml_update_count = 0

        for file in flist: 
            guid = file[:-9]
            xml_status = db.fetchone_xml(guid)
            if xml_status != None: 
                if xml_status[1] != 1:
                    index = xml_status[0]
                    db.update_column(tablename, 'xml_created', 1, index)
                    xml_status_msg = f" \n\
                                                DB updated on crosscheck - \n\
                                                rowid: {index}, \n\
                                                guid: {guid} \n\
                                                xml_created: 1 \n"
                    logger.info(xml_status_msg)
                    xml_update_count += 1
                else:
                    xml_pass_msg = f"{guid} xml_status already = 1 "
                    logger.debug(xml_pass_msg)
                    pass
            else: 
                none_msg = f"{guid} was not found in the DB."
                logger.error(none_msg)
                pass
        
        xml_count_msg = f"Total Count for the xml status update = {xml_update_count}"
        logger.info(xml_count_msg)
    
        for root, dirs, files in os.walk(proxy_storage_path):
            for p in files:
                if p.endswith(".mov"):
                    plist.append(p)
                else:
                    pass

        proxytocheck = len(plist)

        proxy_check_msg = f"Total number of proxy files to crosscheck against DB:  {proxytocheck}"
        logger.info(proxy_check_msg)

        proxy_update_count = 0

        for file in plist:
            guid = file[:-4]
            proxy_status = db.fetchone_proxy(guid)
            if proxy_status != None: 
                if proxy_status[1] != 1:
                    index = proxy_status[0]
                    db.update_column(tablename, 'proxy_copied', 1, index)
                    proxy_status_msg = f" \n\
                                            DB updated on crosscheck - \n\
                                            rowid: {index}, \n\
                                            guid: {guid} \n\
                                            proxy_copied: 1 \n"
                    logger.info(proxy_status_msg)
                    proxy_update_count += 1
                else:
                    proxy_pass_msg = f"{guid} proxy_status already = 1 "
                    logger.debug(proxy_pass_msg)
                    pass
            else: 
                none_msg = f"{guid} was not found in the DB."
                logger.error(none_msg)

        proxy_update_msg = f"Total number of files with proxy status updated:  {proxy_update_count}"
        logger.info(proxy_update_msg)

        crosscheck_complete_msg = f"XML AND PROXY FS-CROSSCHECK COMPLETE"
        logger.info(crosscheck_complete_msg)
        
        return 

    except Exception as e:
        cc_excp_msg = f"\n\
        Exception raised on the asset fs-crosscheck.\n\
        Error Message:  {str(e)} \n"
        logger.exception(cc_excp_msg)


if __name__ == '__main__':
    crosscheck_db('assets')
