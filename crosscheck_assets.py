
#! /usr/bin/env python3

import logging
import os

import config as cfg
import pandas as pd

import database as db

logger = logging.getLogger(__name__)


def crosscheck_assets(tablename): 
    """
    check db records against the full directory list of xmls. update the records as needed. 
    """

    config = cfg.get_config()
    xml_checkin_path = config['paths']['xml_checkin_path']
    proxy_storage_path = config['paths']['proxy_storage_path']

    flist = []
    plist = []

    try:
        for root, dirs, files in os.walk(xml_checkin_path):
            for f in files:
                if f.find("test") != -1:
                    pass
                elif not f.endswith(".xml_DONE"):
                    pass
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
                    db.update_row(tablename, 'xml_created', 1, index)
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
                print(none_msg)
                logger.error(none_msg)
            
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
                    db.update_row(tablename, 'proxy_copied', 1, index)
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

    except Exception as e:
        cc_excp_msg = f"\n\
        Exception raised on the asset crosscheck.\n\
        Error Message:  {str(e)} \n"

        logger.exception(cc_excp_msg)


if __name__ == '__main__':
    crosscheck_assets('assets')
