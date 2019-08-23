#! /usr/bin/env python3

import logging

logger = logging.getLogger(__name__)


def get_user_input():

    while True:
        xml_val = str(input(f"How many xmls / proxies are needed in this batch?  "))
        try:
            if int(xml_val) > 10000:
                print(f"{xml_val} is not a valid entry for the starting index, try again.")
                continue
            else:
                xml_total = xml_val
                xml_info_msg = (f"{xml_total} selected value for xml creation.")
                break

        except ValueError as e:
            xml_excp_msg = f"\n\
            ValueError raised for xml value: {xml_val}.\n\
            Error Message:  {str(e)} \n\
            "
            print(f"{xml_val} is not a valid entry for the starting index, try again.")
            logger.error(xml_excp_msg)
            continue

    while True:
        get_db = str(
            input(f"Export a new copy of the Gor DB? [Y/N]  "))

        try:
            if str(get_db.upper()) in ["Y", "YES"]:
                getnew_db = True
            elif str(get_db.upper()) in ["N", "NO"]:
                getnew_db = False
            else:
                print("Not a valid entry, please try again.")
                continue
            getdb_info_msg = (f"Create a new DB export: {getnew_db}")
            break

        except ValueError as e:
            getnew_db_excp_msg = f"\n\
            ValueError raised for the getnew_db value: {getnew_db_excp_msg}.\n\
            Error Message:  {str(e)} \n\
            "
            print(f"{str(get_db)} is not a valid response, it must be Yes or No.")
            logger.error(getnew_db_excp_msg)
            continue

    logger.info(xml_info_msg)
    logger.info(getdb_info_msg)
    return xml_total, getnew_db


if __name__ == '__main__':
    get_user_input()
