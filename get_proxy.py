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

    file_list = [name for name in os.listdir(xmlpath)]

    for xml in file_list:

        if xml == '.DS_Store':
            os.remove(xmlpath + '.DS_Store')

        else:
            xml_fpath = os.path.join(xmlpath, xml)

            tree = ET.parse(xml_fpath)
            root = tree.getroot()

            titletype = root[0][8].text

            if titletype != "video":
                tt_info_msg = f"{xml} titletype is not 'video', skipping get_proxy."
                logger.info(tt_info_msg)
                pass
                # set up a move for the XML if it is an archive

            else:
                guid_x = xml.replace("-", "")
                guid = guid_x[24:-4]
                proxy_fn = xml[:-4] + '.mov'
                n = 2

                glist = [guid[i:i+n] for i in range(0, len(guid), n)]

                full_proxypath = os.path.join(proxypath, glist[2], glist[3], proxy_fn)

                if os.path.exists(full_proxypath) is not True:
                    proxy_err_msg = f"Proxy path does not exist. \
                    {full_proxypath}"
                    logger.error(proxy_err_msg)

                else:
                    try:
                        print("")
                        pcopy = subprocess.Popen(["rsync", "-vaE", "--progress", full_proxypath, tmp_checkin],
                        stderr=subprocess.PIPE, stdout=subprocess.PIPE, universal_newlines=True)
                        stdout, stderr = pcopy.communicate()
                        logger.info(stdout)
                        logger.error(stderr)
                        
                        xcopy = subprocess.Popen(["rsync", "-vaE", "--progress", tmp_checkin, xml],
                        stderr=subprocess.PIPE, stdout=subprocess.PIPE, universal_newlines=True)
                        stdout, stderr = xcopy.communicate()
                        logger.info(stdout)
                        logger.error(stderr)

                        proxy_copied = 1
                        db.update_row(index, proxy_copied)
                        
                        proxy_cp_msg = f"{proxy_fn} was copied to the dalet tmp."
                        xml_mv_msg = f"{xml} was moved to the dalet tmp."
                        logger.info(xml_mv_msg)
                        logger.info(proxy_cp_msg)

                    except Exception as e:
                        proxy_excp_msg = f"\n\
                        Exception raised on the Proxy copy.\n\
                        Error Message:  {str(e)} \n\
                        "
                        logger.exception(proxy_excp_msg)


if __name__ == '__main__':
    get_proxy()
