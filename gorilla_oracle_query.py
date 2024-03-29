#! /usr/bin/env python3

import csv
import logging
import os
from time import localtime, strftime

import cx_Oracle
import yaml

import config as cfg

# cx_Oracle.init_oracle_client(
#     lib_dir="/opt/oracle/instantclient_19_8")

sql_query = """
        SELECT
            essence.guid,
            essence.name,
            essence.filesize,
            essence.datatapeid,
            essencedetail.objectnm,
            essencedetail.contentlength,
            essencedetail.sourcecreatedt,
            essencedetail.createdt,
            essencedetail.lastmdydt,
            essencedetail.timecodein,
            essencedetail.timecodeout,
            essencedetail.onairid,
            filemapping.ruri,
            mediainfo.metaxml
        FROM ESSENCE
        INNER JOIN ESSENCEDETAIL ON essence.guid=essencedetail.guid
        INNER JOIN FILEMAPPING ON filemapping.guid=essence.guid
        LEFT OUTER JOIN MEDIAINFO ON mediainfo.guid=filemapping.guid
        WHERE ESSENCE.ISGARBAGE=0
       --ROWNUM < 1000
        """

fieldnames = [
    "GUID",
    "NAME",
    "FILESIZE",
    "DATATAPEID",
    "OBJECTNM",
    "CONTENTLENGTH",
    "SOURCECREATEDT",
    "CREATEDT",
    "LASTMDYDT",
    "TIMECODEIN",
    "TIMECODEOUT",
    "ONAIRID",
    "RURI",
    "METAXML",
]

logger = logging.getLogger(__name__)


def buildcsv(date):
    """
    Creates a CSV export from the Oracle DB for the Gorilla MAM. Uses the sql_query and fieldnames list to define the required fields.
    """

    config = cfg.get_config()

    root_path = config["paths"]["root_path"]
    csv_path = config["paths"]["csv_path"]

    db_user = config["oracle-db-gor"]["user"]
    db_pass = config["oracle-db-gor"]["pass"]
    db_url = config["oracle-db-gor"]["url"]

    os.chdir(csv_path)

    try:
        row_count = 0
        connection = cx_Oracle.connect(db_user, db_pass, db_url)
        cursor = connection.cursor()
        results = cursor.execute(sql_query)
        gor_csv = date + "_" + "gorilla_db_export.csv"

        writer = csv.writer(open(gor_csv, "w", newline=""))

        export_1_msg = f"START GORILLA DB EXPORT"
        logger.info(export_1_msg)

        writer.writerow(fieldnames)

        for row in results:
            print("")
            print(str(row))
            print("")
            writer.writerow(row)
            row_count += 1

        export_2_msg = f"\n\
        ==================================================================\n\
                            GORILLA DB EXPORT Complete \n\
                    {str(strftime('%A, %d. %B %Y %I:%M%p', localtime()))} \n\
                    Rows Exported: {str(row_count)}\n\
        ==================================================================\
        "

        logger.info(export_2_msg)

        connection.close()

        os.chdir(root_path)

        return gor_csv

    except Exception as e:

        db_export_excp_msg = f"\n\
        Exception raised on the Gorilla DB Export.\n\
        Error at DB Row: {row_count}\n\
        Error Message:  {str(e)} \n\
        "

        logger.exception(db_export_excp_msg)


if __name__ == "__main__":
    buildcsv()
