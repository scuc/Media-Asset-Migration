#! /usr/bin/env python3

import csv
import logging
import os
from time import localtime, strftime

import cx_Oracle
import yaml

import config as cfg

cx_Oracle.init_oracle_client(
    lib_dir="/opt/oracle/instantclient_19_8")

sql_query = '''
        SELECT
            dp_archived_objects.ao_id,
            dp_archived_objects.ao_UUID,
            dp_archived_objects.ao_object_name,
            dp_archived_objects.ao_comment,
            dp_archived_objects.ao_category,
            dp_archived_objects.ao_date_archive,
            dp_archived_objects.ao_last_read,
            dp_archived_objects.ao_object_size,
            dp_object_components.oc_component_name,
            dp_object_components.oc_component_is_deleted,
            dp_object_instances.on_category,
            dp_object_instances.on_media_name,
            dp_object_instances.on_date_creation,
            dp_object_instances.on_last_access_time,
            dp_checksums.ch_checksum_value,
            dp_checksums.ch_checksum_date,
            dp_checksum_types.cy_checksum_type
        FROM DP_ARCHIVED_OBJECTS
        INNER JOIN DP_OBJECT_INSTANCES ON DP_ARCHIVED_OBJECTS.AO_OBJECT_NAME=DP_OBJECT_INSTANCES.ON_OBJECT_NAME
        INNER JOIN DP_OBJECT_COMPONENTS ON DP_ARCHIVED_OBJECTS.AO_ID=DP_OBJECT_COMPONENTS.OC_OBJECT_AO_ID
        INNER JOIN DP_CHECKSUMS ON DP_ARCHIVED_OBJECTS.AO_ID=DP_CHECKSUMS.CH_OBJECT_AO_ID
        INNER JOIN DP_CHECKSUM_TYPES ON DP_CHECKSUMS.CH_TYPE_CY_ID=DP_CHECKSUM_TYPES.CY_ID
        WHERE AO_CATEGORY = 'TACS-DIVA'
        '''

fieldnames = ['AO_ID', 'AO_UUID', 'GUID', 'AO_COMMENT',
              'AO_CATEGORY', 'AO_DATE_ARCHIVE', 'AO_LAST_READ',
              'AO_OBJECT_SIZE', 'OC_COMPONENT_NAME', 'OC_COMPONENT_IS_DELETED',
              'ON_CATEGORY', 'ON_MEDIA_NAME', 'ON_DATE_CREATION',
              'ON_LAST_ACCESS_TIME', 'CH_CHECKSUM_VALUE', 'CH_CHECKSUM_DATE',
              'CY_CHECKSUM_TYPE']

logger = logging.getLogger(__name__)


def buildcsv(date):
    """
    Creates a CSV export from the Oracle DB for Diva Archive. Uses the sql_query and fieldnames list to define the required fields.
    """

    config = cfg.get_config()

    rootpath = config['paths']['rootpath']
    csvpath = config['paths']['csvpath']

    db_user = config['oracle-db-diva']['user']
    db_pass = config['oracle-db-diva']['pass']
    db_url = config['oracle-db-diva']['url']

    os.chdir(csvpath)

    try:
        row_count = 0
        connection = cx_Oracle.connect(db_user, db_pass, db_url)
        cursor = connection.cursor()
        results = cursor.execute(sql_query)

        diva_csv = (date + "_" + "diva_db_export.csv")
        writer = csv.writer(open(diva_csv, 'w', newline=''))

        export_1_msg = f"START DIVA DB EXPORT"
        logger.info(export_1_msg)

        writer.writerow(fieldnames)

        for row in results:
            print("")
            print(str(row))
            print("")
            writer.writerow(row)
            row_count += 1

        connection.close()

        export_2_msg = f"\n\
        ==================================================================\n\
                            DIVA DB EXPORT Complete \n\
                    {str(strftime('%A, %d. %B %Y %I:%M%p', localtime()))} \n\
                    Rows Exported: {str(row_count)}\n\
        ==================================================================\
        "

        logger.info(export_2_msg)

        os.chdir(rootpath)

        return diva_csv

    except Exception as e:
        db_export_excp_msg = f"\n\
        Exception raised on the Diva DB Export.\n\
        Error at DB Row: {row_count}\n\
        Error Message:  {str(e)} \n\
        "

        logger.exception(db_export_excp_msg)

if __name__ == '__main__':
    buildcsv()
