#! /usr/bin/env python3

import logging
import os
import shutil

import pandas as pd

import config as cfg
import crosscheck_assets as cca
import database as db
import get_mediainfo as gmi

logger = logging.getLogger(__name__)


def update_db(date, tablename, clean_csv=None):
    """
    Start by creating a backup of the exisiting DB.
    Then update the DB by comparing rows in new CSV export to rows in the existing DB.
    Add new rows from the CSV into the DB, and remove rows from the DB if they do not exist in
    the new CSV.
    """

    config = cfg.get_config()
    root_path = config["paths"]["root_path"]
    csv_path = config["paths"]["csv_path"]

    if clean_csv is None:
        clean_csv = date + "_" + "gor_diva_merged_cleaned.csv"
    else:
        pass

    if os.path.isfile(os.path.join(root_path, "database.db")) is not True:
        return

    else:
        try:
            shutil.copy2(
                os.path.join(root_path, "database.db"),
                os.path.join(root_path, "database_BKP_" + date + ".db"),
            )

            update_db_msg = f"BEGIN DB UPDATE"
            logger.info(update_db_msg)
            print(update_db_msg)

            cca.crosscheck_assets(tablename)
            os.chdir(csv_path)

            with open(clean_csv, mode="r", encoding="utf-8-sig") as c_csv:

                pd_reader = pd.read_csv(c_csv, header=0)
                df = pd.DataFrame(pd_reader)

                update_count = 0
                update_index = []
                drop_count = 0
                drop_index = []
                insert_count = 0
                insert_index = []
                mismatch_count = 0
                mismatch_index = []
                total_count = 0
                none_count = 0

                for index, row in df.iterrows():

                    os.chdir(root_path)

                    guid = str(row["GUID"])
                    titletype = str(row["TITLETYPE"])
                    datatapeid = str(row["DATATAPEID"])

                    update_db_msg_01 = (
                        f"Updating DB for (Index, GUID): ({index}, {guid})"
                    )
                    logger.info(update_db_msg_01)
                    print(str(index) + "    " + guid)

                    db_row_id = db.fetchone_guid(
                        guid
                    )  # fetch db row based on GUID, row ID may not match db Row ID.

                    if db_row_id is not None:
                        db_row = db.select_row(db_row_id[0])

                    else:
                        db_row_msg = f"None value(s) found in db row, skipping this row. \n {db_row}"
                        none_count += 1
                        continue

                    db_datatapeid = db_row[4]
                    db_aoid = db_row[24]
                    db_titletype = db_row[14]

                    if (
                        guid == db_row[1]
                        and db_datatapeid == "NULL"
                        and db_aoid == "NULL"
                    ):
                        db.update_row("assets", index, row)
                        update_count += 1
                        update_index.append(index)

                    if guid != db_row[1] and db.fetchone_guid(guid) is None:
                        db.drop_row("assets", index, guid)
                        drop_count += 1
                        drop_index.append(index)

                    if db_row is None and row["_merge"] == "both":
                        db.insert_row(index, row)
                        insert_count += 1
                        insert_index.append(index)

                    if titletype != db_titletype:
                        db.update_column("assets", "TITLETYPE", titletype, index)
                        update_count += 1
                        update_index.append(index)

                    if guid != db_row[1] and db.fetchone_guid(guid) != None:
                        mismatch_msg = (
                            f"Mismatch in the db update: {db_row[1]} != {guid}"
                        )
                        logger.error(mismatch_msg)
                        mismatch_count += 1
                        mismatch_index.append(index)
                        pass

                    else:
                        nochange_msg = f"No change to {guid} at row index {index}."
                        logger.debug(nochange_msg)
                        # print(nochange_msg)
                        pass

                    total_count += 1

            update_summary_msg = f"\n\
                                    Update Count:  {update_count}\n\
                                    Drop Count: {drop_count}\n\
                                    Insert Count: {insert_count}\n\
                                    Mismatch Count: {mismatch_count}\n\
                                    No Change Count: {total_count - (update_count + drop_count + insert_count + mismatch_count)}\n\
                                    Total Count: {total_count}\n\
                                    "

            index_summary_msg = f"\n\
                                None Value Count = {none_count}\n\
                                update index: {update_index}\n\
                                drop index: {drop_index}\n\
                                insert index: {insert_index}\n\
                                mismatch index: {mismatch_index}\n\
                                "
            logger.info(update_summary_msg)
            logger.info(index_summary_msg)

            print(update_summary_msg)
            print("")
            print(index_summary_msg)

            db_update_complete_msg = f"DB UPDATE COMPLETE"
            logger.info(db_update_complete_msg)

        except Exception as e:
            dbupdate_err_msg = f"Error updating the DB."
            logger.exception(dbupdate_err_msg)


if __name__ == "__main__":
    update_db(
        "202404021315",
        "assets",
        clean_csv="",
    )
