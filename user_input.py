#! /usr/bin/env python3

import logging

logger = logging.getLogger(__name__)


def get_user_input():

    while True:
        xml_val = str(
            input(f"How many xmls / proxies are needed in this batch?  "))
        try:
            if int(xml_val) > 10000:
                print(
                    f"{xml_val} is not a valid entry for the starting index, try again.")
                continue
            else:
                xml_total = xml_val
                xml_info_msg = (
                    f"Selected value for xml creation: {xml_total}")
                break

        except ValueError as e:
            xml_excp_msg = f"\n\
            ValueError raised for xml value: {xml_val}.\n\
            Error Message:  {str(e)} \n\
            "
            print(
                f"{xml_val} is not a valid entry for the starting index, try again.")
            logger.exception(xml_excp_msg)
            continue

    while True:
        response2 = str(input(f"Export a new copy of the Gor DB? [Y/N]  "))

        try:
            response2 = yesno_rsp(response2)
            if response2 not in [True, False]:
                continue
            else:
                getnew_db = response2
                getdb_info_msg = (f"Export new DB: {getnew_db}")
                break

        except ValueError as e:
            getnew_db_excp_msg = f"\n\
            ValueError raised for the getnew_db value: {response2}.\n\
            Error Message:  {str(e)} \n\
            "
            print(f"{str(get_db)} is not a valid response, it must be Yes or No.")
            logger.exception(getnew_db_excp_msg)
            continue

    while True:
        response3 = str(
            input(f"Crosscheck assets in the existing DB? [Y/N]  "))

        try:
            response3 = yesno_rsp(response3)
            if response3 not in [True, False]: 
                continue
            else: 
                crosscheck_db = response3
                ccdb_info_msg = (f"Crosscheck DB: {getnew_db}")
                break

        except ValueError as e:
            crosscheck_db_excp_msg = f"\n\
            ValueError raised for the crosscheck_db value: {response3}.\n\
            Error Message:  {str(e)} \n\
            "
            print(f"{str(get_db)} is not a valid response, it must be Yes or No.")
            logger.exception(croscheck_db_excp_msg)
            continue

    logger.info(xml_info_msg)
    logger.info(getdb_info_msg)
    logger.info(ccdb_info_msg)
    return xml_total, getnew_db, crosscheck_db


def yesno_rsp(response): 
    if str(response.upper()) in ["Y", "YES"]:
        return True
    elif str(response.upper()) in ["N", "NO"]:
        return False
    else:
        print("Not a valid entry, please try again.")
        return None


if __name__ == '__main__':
    get_user_input()
