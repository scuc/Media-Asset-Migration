#! /usr/bin/env python3

import logging
import os
import subprocess

import database as db

import config as cfg
import xml.etree.ElementTree as ET

logger = logging.getLogger(__name__)


def get_proxy():
    """
    Get the corresponding proxy file for each xml document,
    and copy it to a tmp location for checking staging.
    """

    config = cfg.get_config()
    conn = db.connect()

    xmlpath = config['paths']['xmlpath']
    proxypath = config['paths']['proxypath']
    tmp_checkin = config['paths']['tmp']

    file_list = [name for name in os.listdir(
        xmlpath) if name.startswith(".") != True]

    xml_count = 0
    proxy_count = 0

    for xml in file_list:

        if xml[-4:] != ".xml":
            pass
        else:
            xml_fpath = os.path.join(xmlpath, xml)

            tree = ET.parse(xml_fpath)
            root = tree.getroot()

            titletype = root[0][8].text

            if titletype == "archive":
                tt_info_msg = f"{xml} titletype is not 'video', skipping get_proxy."
                logger.info(tt_info_msg)
                file_copy(xml_fpath, tmp_checkin)
                xml_count += 1

            elif titletype == "video":
                guid_x = xml.replace("-", "")
                guid_r = guid_x[24:-4]
                proxy_fn = xml[:-4] + '.mov'
                guid = xml[:-4]
                
                n = 2
                glist = [guid_r[i:i+n] for i in range(0, len(guid_r), n)]

                proxy_fpath = os.path.join(proxypath, glist[2], glist[3], guid, proxy_fn)

                if os.path.exists(proxy_fpath) is not True:
                    proxy_err_msg = f"Proxy path does not exist. \
                    {proxy_fpath}"
                    logger.error(proxy_err_msg)

                else:
                    try:
                        print("")
                        pcopy = file_copy(proxy_fpath, tmp_checkin)
                         
                        if len(pcopy) == 0: 
                            row = db.fetchone_proxy(guid)
                            rowid = row[0]
                            db.update_column('assets', 'proxy_copied', 1, rowid)
                            proxy_cp_msg = f"{proxy_fn} was copied to the dalet tmp."
                            logger.info(proxy_cp_msg)
                            proxy_count += 1
                        else: 
                            pass
                            proxy_err_cp_msg = f"{proxy_fn} encountered an error on the copy to the dalet tmp."
                            logger.info(proxy_err_cp_msg)
                             
                        xcopy = file_copy(xml_fpath, tmp_checkin)
                            
                        if len(xcopy) == 0: 
                            os.remove(xml_fpath)
                            xml_mv_msg = f"{xml} was moved to the dalet tmp."
                            logger.info(xml_mv_msg)
                            xml_count += 1
                        else: 
                            xml_cp_err_msg = f"{xml} encountered an error on the copy to the dalet tmp."
                            logger.error(xml_cp_err_msg)

                    except Exception as e:
                        proxy_excp_msg = f"\n\
                        Exception raised on the Proxy copy.\n\
                        Error Message:  {str(e)} \n\
                        "
                        logger.exception(proxy_excp_msg)
            else: 
                notitletype_msg = f"TitleType not determined for: {xml}"
                logger.info(notitletype_msg)

        proxy_complete_msg = f"PROXY COPY AND XML MOVE COMPLETE. \n\
                                {proxy_count} proxies copied \n\
                                {xml_count} xmls copied \n"


def file_copy(source, destination): 
    """
    Use rsync to copy files to from source to destination. 
    """
    try:
        copy = subprocess.Popen(["rsync", "-vaE", "--exclude=\".*\"", "--progress", source, destination],
        stderr=subprocess.PIPE, stdout=subprocess.PIPE, universal_newlines=True)
        stdout, stderr = copy.communicate()
        logger.info(stdout)
        if len(stderr) != 0:
            logger.error(stderr)
            return stderr
        else: 
            return stderr

    except Exception as e:
        copy_excp_msg = f"\n\
        Exception raised on the file copy.\n\
        File Name: {source} \n\
        Error Message:  {str(e)} \n\
        "
        logger.exception(proxy_excp_msg)


if __name__ == '__main__':
    get_proxy()
