
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


def get_root(xml_fname):
    tree = ET.parse(fname)
    root = tree.getroot()
    return root


def get_guid():

        xml_fname = f[103:-5]
        print(xml_fname)
        dst_path = rootpath + "_tmp/" + xml_fname
        xml_doc = shutil.copy2(f, dst_path)
        root = get_root(xml_doc)
        guid_elem = root.find('Title/key1')
        guid = guid_elem.text + '.xml'
        os.remove(xml_doc)
        print(guid)
    return guid

def crosscheck_assets(tablename): 
    """
    check db records against the full directory list of xmls. update the records as needed. 
    """

    config = cfg.get_config()
    xml_checkin_path = config['paths']['xml_checkin_path']
    proxy_storage_path = config['paths']['proxy_storage_path']
    rootpath = config['paths']['rootpath']

    flist = []
    plist = []

    crosscheck_db_msg = f"BEGIN XML AND PROXY CROSSCHECK"
    logger.info(crosscheck_db_msg)
    print(crosscheck_db_msg)

    try:
        for root, dirs, files in os.walk(xml_checkin_path):
            for f in files:
                if f.find("test") != -1:
                    pass
                elif not f.endswith(".xml_DONE"):
                    pass
                elif (re.search(r"\(\d+\)", f) is not None
                      and os.file.endswith("_DONE")): 
                    guid = get_guid(f)
                    flist.append(guid)
                else:
                    flist.append(f)
        xmltocheck = len(flist)
        print(f"Total number of XML files to crosscheck against DB:  {xmltocheck}")
        logger.info(f"Total number of XML files to crosscheck against DB:  {xmltocheck}")


        for file in flist: 
            guid = file[:-9]
            xml_status = db.fetchone_xml(guid)
            if xml_status != None: 
                if xml_status[1] != 1:
                    index = xml_status[0]
                    db.update_column(tablename, 'xml_created', 1, index)
                    xml_status_msg = f" \
                                            DB updated on crosscheck - \n\
                                            rowid: {index}, \n\
                                            guid: {guid} \n\
                                            xml_created: 1 \n"
                    print(xml_status_msg)
                    logger.info(xml_status_msg)
                else:
                    print(f"{guid} xml_status already = 1 ")
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
                    proxy_status_msg = f" \
                                            DB updated on crosscheck - \n\
                                            rowid: {index}, \n\
                                            guid: {guid} \n\
                                            proxy_copied: 1 \n"
                    print(proxy_status_msg)
                    logger.info(proxy_status_msg)
                else:
                    print(f"{guid} proxy_status already = 1 ")
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
