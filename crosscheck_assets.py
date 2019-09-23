
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
    print("FILE:   " + f)
    xml_fname = f[:-5]
    print("XML_FNAME:   " + xml_fname)
    dst_path = rootpath + "_tmp/" + xml_fname
    xml_doc = shutil.copy2(f_path, dst_path)
    root = get_root(xml_doc)
    guid_elem = root.find('Title/key1')
    guid = guid_elem.text + '.xml_DONE'
    os.remove(xml_doc)
    print("GUID: " + guid)
    return guid

def crosscheck_assets(tablename): 
    """
    Check db records against the full directory list of xmls. update the records as needed. 
    """

    flist = []
    plist = []

    crosscheck_db_msg = f"BEGIN XML AND PROXY CROSSCHECK"
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
        logger.info(f"Total number of XML files to crosscheck against DB:  {xmltocheck}")

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
                else:
                    xml_pass_msg = f"{guid} xml_status already = 1 "
                    logger.debug(xml_pass_msg)
                    pass
            else: 
                none_msg = f"{guid} was not found in the DB."
                print(xml_status)
                print(none_msg)
                logger.error(none_msg)
                pass
            
        for root, dirs, files in os.walk(proxy_storage_path):
            for p in files:
                if p.endswith(".mov"):
                    plist.append(p)
                else:
                    pass

        proxytocheck = len(plist)
        print(f"Total number of proxy files to crosscheck against DB:  {proxytocheck}")
        logger.info(f"Total number of proxy files to crosscheck against DB:  {proxytocheck}")

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
                else:
                    proxy_pass_msg = f"{guid} proxy_status already = 1 "
                    logger.debug(proxy_pass_msg)
                    pass
            else: 
                none_msg = f"{guid} was not found in the DB."
                print(none_msg)
                logger.error(none_msg)

        crosscheck_complete_msg = f"XML AND PROXY CROSSCHECK COMPLETE"
        logger.info(crosscheck_complete_msg)
        print(crosscheck_complete_msg)
        
        return 

    except Exception as e:
        cc_excp_msg = f"\n\
        Exception raised on the asset crosscheck.\n\
        Error Message:  {str(e)} \n"

        logger.exception(cc_excp_msg)


if __name__ == '__main__':
    crosscheck_assets('assets')
