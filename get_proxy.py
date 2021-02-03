#! /usr/bin/env python3

import logging
import os
import subprocess

import database as db

import config as cfg
# import xml.etree.ElementTree as ET

logger = logging.getLogger(__name__)


def get_proxy(proxy_total):
    """
    Get a set of files where titletype = video and proxy copy status is 0,
    Copy the proxies a tmp location for checkin staging.
    PROXY_COPIED values: 
    0 = proxy not copied
    1 = proxy copied
    2 = proxy path does not exist 
    3 = no proxy, content type is archive 
    """

    config = cfg.get_config()
    conn = db.connect()

    xmlpath = config['paths']['xmlpath']
    proxypath = config['paths']['proxypath']
    tmp_checkin = config['paths']['tmp']
    rootpath = config['paths']['rootpath']

    rows = db.fetchall_proxy('assets')
    
    proxy_count = 0

    for row in rows:
        rowid = row[0]
        guid = str(row[1])
        proxy_copied = row[22]
        guid_x = guid.replace("-", "")
        guid_r = guid_x[24:]
        proxy_fn = guid + '.mov'

        """
        Use the parts GUID to generate a list that will be used to build the path to the proxy.
        """
        n = 2
        glist = [guid_r[i:i+n] for i in range(0, len(guid_r), n)]

        proxy_fpath = os.path.join(
            proxypath, glist[2], glist[3], guid, proxy_fn)
        
        if (proxy_count < int(proxy_total)
            and proxy_copied == 0 
            and os.path.exists(proxy_fpath) is True):
            
            try:
                pcopy = file_copy(proxy_fpath, tmp_checkin)
                
                if len(pcopy) == 0: 
                    row = db.fetchone_proxy(guid)
                    db.update_column('assets', 'proxy_copied', 1, rowid)
                    proxy_cp_msg = f"{proxy_fn} was copied to the dalet tmp."
                    logger.info(proxy_cp_msg)
                    proxy_count += 1
                else: 
                    pass
                    proxy_err_cp_msg = f"{proxy_fn} encountered an error on the copy to the dalet tmp."
                    logger.info(proxy_err_cp_msg)
                    
            except Exception as e:
                proxy_excp_msg = f"\n\
                Exception raised on the Proxy copy.\n\
                Error Message:  {str(e)} \n\
                "
                logger.exception(proxy_excp_msg)
                break
        else:
            if os.path.exists(proxy_fpath) is not True:
                proxy_err_msg = f"Proxy path does not exist. \n\
                {proxy_fpath}"
                logger.error(proxy_err_msg)
                db.update_column('assets', 'proxy_copied', 2, rowid)
                continue

    os.chdir(rootpath)
    proxy_complete_msg = f"PROXY COPY COMPLETE. \n\
                        {proxy_count} proxies copied \n"

    logger.info(proxy_complete_msg)

    return


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
        logger.exception(copy_excp_msg)


if __name__ == '__main__':
    get_proxy(1)
