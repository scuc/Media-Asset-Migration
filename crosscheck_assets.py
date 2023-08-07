#! /usr/bin/env python3

import logging
import os
import re
import shutil
import xml.etree.ElementTree as ET

import pandas as pd

import config as cfg
import database as db

logger = logging.getLogger(__name__)

config = cfg.get_config()
xml_checkin_path = config["paths"]["xml_checkin_path"]
xml_storage_paths = config["paths"]["xml_storage_paths"]
proxy_storage_path = config["paths"]["proxy_storage_path"]
proxy_tmp_path = config["paths"]["proxy_tmp_path"]
proxy_failed_path = config["paths"]["proxy_failed_path"]
root_path = config["paths"]["root_path"]


def get_root(xml_doc):
    """
    Get the root of an xml document for parsing element tree.
    """
    tree = ET.parse(xml_doc)
    root = tree.getroot()
    return root


def get_guid(f, f_path):
    """
    When the filename for the xml has the wrong guid, get the guid from an element of the xml
    """
    xml_fname = f[:-5]
    dst_path = root_path + "_tmp/" + xml_fname
    xml_doc = shutil.copy2(f_path, dst_path)
    root = get_root(xml_doc)
    guid_elem = root.find("Title/key1")
    xml_m = guid_elem.text + ".xml_DONE"
    os.remove(xml_doc)
    return xml_m


def get_checkedin_assets():
    """
    Loop through a directory with xml or proxy files, and build a list of each.
    """
    xml_list = []
    for path in xml_storage_paths:
        for xml in os.listdir(path):
            if (
                xml.startswith(".")
                or xml.endswith("_DONE") == False
                or xml.find("test") == 0
            ):
                pass
            elif (
                re.search(r"\(\d+\)", xml) != None
                and xml.endswith("_DONE")
                and xml.startswith(".") == False
            ):
                xml_m = get_guid(xml, os.path.join(path, xml))
                xml_list.append(xml_m)
            else:
                xml_list.append(xml)

    xml_list_msg = f"XML list complete. Total checked in XML = {len(xml_list)}"
    logger.info(xml_list_msg)

    proxy_list = []
    combined_proxy = (
        os.listdir(proxy_storage_path)
        + os.listdir(proxy_tmp_path)
        + os.listdir(proxy_failed_path)
    )
    for proxy in combined_proxy:
        if proxy.startswith("."):
            pass
        else:
            proxy_list.append(proxy)

    proxy_list_msg = (
        f"Proxy list complete. Total checked in Proxies = {len(proxy_list)}"
    )
    logger.info(proxy_list_msg)

    return xml_list, proxy_list


def crosscheck_db(tablename):
    """
    Check db rows against the xmls and proxies in the filesystem. Update the DB records as needed.
    """
    crosscheck_db_start_msg = f"BEGIN DB-CROSSCHECK"
    logger.info(crosscheck_db_start_msg)

    try:
        xml_list, proxy_list = get_checkedin_assets()
        rows = db.fetchall(tablename)
        for row in rows:
            xmlname = row[1] + ".xml_DONE"
            if row[4] == "NULL":
                pass
            elif xmlname in xml_list and row[21] == 0:
                db.update_column(tablename, "XML_CREATED", 1, row[0])
                xml_update_msg = (
                    row[1] + "  xml status updated in the db. XML_CREATED = 1"
                )
                logger.info(xml_update_msg)
            else:
                pass
                # db.update_column(tablename, 'XML_CREATED', 0, row[0])
                # xml_update_msg = row[1] + "  xml status updated in the db. XML_CREATED = 0"
                # logger.debug(xml_update_msg)

            proxyname = row[1] + ".mov"

            if proxyname in proxy_list and row[22] == 0:
                db.update_column(tablename, "PROXY_COPIED", 1, row[0])
                proxy_update_msg = row[1] + "  proxy status updated. PROXY_COPIED = 1"
                logger.info(proxy_update_msg)
            elif row[14] == "archive" and row[22] == 0:
                db.update_column(tablename, "PROXY_COPIED", 3, row[0])
                proxy_update_msg = (
                    row[1] + "  title type is "
                    "archive"
                    ", proxy status updated. PROXY_COPIED = 3"
                )
                logger.info(proxy_update_msg)
            else:
                pass
                # db.update_column(tablename, 'PROXY_COPIED', 0, row[0])
                # proxy_update_msg = row[1] + "  proxy status updated in the db. PROXY_COPIED = 0"
                # logger.debug(proxy_update_msg)

        crosscheck_db_end_msg = f"DB-CROSSCHECK COMPLETE"
        logger.info(crosscheck_db_end_msg)

    except Exception as e:
        cc_excp_msg = f"\n\
        Exception raised on the asset db-crosscheck.\n\
        Index:  {row['ROWID']}\n\
        Error Message:  {str(e)} \n"
        logger.exception(cc_excp_msg)


def crosscheck_assets(tablename):
    """
    Check files in the filesystem against DB rows. Update the DB records as needed.
    Performs the same check as crosscheck_db(), but from the reverse direction (filesystem to db).
    """

    fs_crosscheck_start_msg = f"BEGIN FS-CROSSCHECK"
    logger.info(fs_crosscheck_start_msg)

    try:
        xml_list, proxy_list = get_checkedin_assets()

        xmltocheck = len(xml_list)

        xml_check_msg = (
            f"Total number of XML files to crosscheck against DB:  {xmltocheck}"
        )
        logger.info(xml_check_msg)

        xml_update_count = 0
        xml_not_found_count = 0

        for xml in xml_list:
            xml_name_msg = f"Checking XML: {xml}"
            logger.info(xml_name_msg)
            guid = xml[:-9]
            xml_status = db.fetchone_xml(guid)
            print(f"XML Status: {str(xml_status)}")
            xml_status_msg = f"XML Status: {str(xml_status)}"
            logger.info(xml_status_msg)

            if xml_status is None:
                none_msg = f"{guid} was not found in the DB.\n\
                xml_list filename: {xml}"
                xml_not_found_count += 1
                logger.info(none_msg)
                continue

            elif xml_status is not None and xml_status[1] == 1:
                xml_pass_msg = f"{guid} xml_status already = 1 "
                logger.debug(xml_pass_msg)

            elif xml_status is not None and xml_status[1] == 0:
                index = xml_status[0]
                db.update_column(tablename, "XML_CREATED", 1, index)
                xml_status_msg = f" \n\
                                            DB updated on crosscheck - \n\
                                            rowid: {index}, \n\
                                            guid: {guid} \n\
                                            xml_created: 1 \n"
                logger.info(xml_status_msg)
                xml_update_count += 1

            else:
                pass_msg = f"No conditions satisfied for: {xml}"
                logger.info(pass_msg)
                xml_update_count += 1
                continue

        xml_count_msg = f"Total Count for the xml status update = {xml_update_count}"
        xml_not_found_count = (
            f"Total Count for the xml not found in db = {xml_not_found_count}"
        )
        logger.info(xml_count_msg)
        logger.info(xml_not_found_count)

        proxytocheck = len(proxy_list)

        proxy_check_msg = (
            f"Total number of proxy files to crosscheck against DB:  {proxytocheck}"
        )
        logger.info(proxy_check_msg)

        proxy_update_count = 0
        proxy_not_found_count = 0

        for proxy in proxy_list:
            guid = proxy[:-4]
            proxy_status = db.fetchone_proxy(guid)

            if proxy_status is None:
                none_msg = f"{guid} was not found in the DB.\n\
                            proxy_list filename: {proxy}"
                proxy_not_found_count += 1
                logger.info(none_msg)
                continue

            elif proxy_status is not None and proxy_status[1] == 1:
                proxy_pass_msg = f"{guid} proxy_status already = 1"
                logger.debug(proxy_pass_msg)

            elif proxy_status is not None and proxy_status[1] == 0:
                index = proxy_status[0]
                db.update_column(tablename, "PROXY_COPIED", 1, index)
                proxy_status_msg = f" \n\
                                        DB updated on crosscheck - \n\
                                        rowid: {index}, \n\
                                        guid: {guid} \n\
                                        proxy_copied: 1 \n"
                logger.info(proxy_status_msg)
                proxy_update_count += 1

            else:
                pass_msg = f"No conditions satisfied for: {xml}"
                logger.info(pass_msg)
                xml_update_count += 1
                continue

        proxy_update_msg = (
            f"Total number of files with proxy status updated:  {proxy_update_count}"
        )
        proxy_not_found_count = (
            f"Total Count for the proxies not found in db = {proxy_not_found_count}"
        )
        logger.info(proxy_update_msg)
        logger.info(proxy_not_found_count)

        fs_crosscheck_complete_msg = f"FS-CROSSCHECK COMPLETE"
        logger.info(fs_crosscheck_complete_msg)

        return

    except Exception as e:
        cc_excp_msg = f"\n\
        Exception raised on the asset fs-crosscheck.\n\
        Error Message:  {str(e)} \n"
        logger.exception(cc_excp_msg)


if __name__ == "__main__":
    crosscheck_assets("assets")
