#! /usr/bin/env python3

import logging
import os
import shutil

import config as cfg
import pandas as pd

import crosscheck_assets as cca
import database as db
import get_mediainfo as gmi

logger = logging.getLogger(__name__)


def update_db(date, tablename):
    """
    Compare rows in new CSV export to rows in the existing DB. 
    """

    config = cfg.get_config()
    rootpath = config['paths']['rootpath']
    clean_csv = date + "_" + "gor_diva_merged_cleaned.csv"

    if os.path.isfile(os.path.join(rootpath, 'database.db')) is not True: 
        return

    else:
        try: 
            shutil.copy2(os.path.join(rootpath, 'database.db'), os.path.join(rootpath, 'database_BKP_'+ date + '.db'))
            
            update_db_msg = f"BEGIN DB UPDATE"
            logger.info(update_db_msg)
            print(update_db_msg)

            cca.crosscheck_assets(tablename)
            os.chdir(rootpath + "_CSV_Exports/")
            
            with open(clean_csv, mode='r', encoding='utf-8-sig') as c_csv:

                pd_reader = pd.read_csv(m_csv, header=0)
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

                for index, row in df.iterrows():

                    guid = str(row['GUID'])
                    print(str(index) + "    " + guid)

                    datatapeid = str(row['DATATAPEID'])

                    db_row = db.select_row(index)
                    db_datatapeid = db_row[4]
                    db_aoid = db_row[24]

                    if (guid == db_row[1]
                        and db_datatapeid == "Null"
                        and db_aoid == "Null"):
                        db.update_row("assets", index, row)
                        update_count += 1
                        update_index.append(index)

                    elif (guid != db_row[1]
                        and db.fetchone_guid(guid) == None):
                        db.drop_row('assets', index, guid)
                        drop_count += 1
                        drop_index.append(index)

                    elif (db_row == None
                        and row['_merge'] == 'both'): 
                        db.insert_row(index, row)
                        insert_count += 1
                        insert_index.append(index)

                    elif (guid != db_row[1]
                        and db.fetchone_guid(guid) != None):
                        mismatch_msg = f"Mismatch in the db update: {db_row[1]} != {guid}"
                        logger.error(mismatch_msg)
                        mismatch_count += 1
                        mismatch_index.append(index)
                        pass

                    else: 
                        nochange_msg = f"No change to {guid} at row index {index}."
                        logger.error(update_error_msg)
                        pass
                    
                    total_count += 1

            update_summary_msg = f"\n\
                                Update Count:  {update_cout}\n\
                                Drop Count: {drop_count}\n\
                                Insert Count: {insert_count}\n\
                                Mismatch Count: {mismatch_count}\n\
                                No Change Count: {total_count - (update_count + drop_count + insert_count + mismatch_count)}\n\
                                Total Count: {total_count}\n\
                                "

            index_summary_msg = f"\n\
                            update index: {update_index}\n\
                            drop index: {drop_index}\n\
                            insert index: {insert_index}\n\
                            mismatch index: {mismatch_index}\n\
                            "
            logger.info(update_summary_msg)
            logger.info(update_index_msg)
        
            db_update_complete_msg = f"DB UPDATE COMPLETE"
            logger.info(db_update_complete_msg)

        except Exception as e:
            dbupdate_err_msg = f"Error updating the DB."
            logger.exception(dbupdate_err_msg)


if __name__ == '__main__':
    update_db('201908271802', 'assets')
