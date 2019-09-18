#! /usr/bin/env python3

import logging
import os

import config as cfg
import pandas as pd

import database as db
import get_mediainfo as gmi

logger = logging.getLogger(__name__)


def update_db(date):
    """
    Compare rows in new CSV export to rows in the existing DB. 
    """

    config = cfg.get_config()

    rootpath = config['paths']['rootpath']

    clean_csv = date + "_" + "gor_diva_merged_cleaned.csv"

    if os.path.isfile(os.join(rootpath, 'database.db')) is not True: 
        return

    else: 
        os.chdir(rootpath + "_CSV_Exports/")

        try: 
            with open(clean_csv, mode='r', encoding='utf-8-sig') as c_csv:

                update_db_msg = f"START UPDATING DB"
                logger.info(update_db_msg)
                print(update_db_msg)

                pd_reader = pd.read_csv(m_csv, header=0)
                df = pd.DataFrame(pd_reader)

                for index, row in df.iterrows():

                    guid = str(row['GUID'])
                    print(str(index) + "    " + guid)

                    datatapeid = str(row['DATATAPEID'])

                    db_row = db.select_row(index)
                    db_datatapeid = db_row[4]
                    db_aoid = db_row[24]

                    if (guid == db_row[1]
                        and db_datatapeid == "Null"):
                        db.update_row("assets", index, row)

                    elif (guid != db_row[1]
                        and db.fetchone_guid(guid) == None):
                        db.drop_row('assets', )
                        pass
                    elif (db_row == None
                        and row['_merge'] == 'both'): 
                        db.insert_row(index, row)
                        
                    elif (guid != db_row[1]
                        and db.fetchone_guid(guid) != None):
                        mismatch_msg = f"Mismatch in the db update: {db_row[1]} != {guid}"
                        logger.error(mismatch_msg)
                        pass


                    elif (guid != db_row[1] 
                    and
                    )

                    else: 

                


         for row in cursor.fetchall():
    stmt='''update Fixture set home_score = :home_score,away_score = :away_score WHERE home_team = :home_team and away_team =:away_team''' 
    cur.execute(stmt,dict(home_score =row[1],away_score=row[2],home_team=row[0],away_team =row[3]))
conn.commit()
print "update fixture done";


if __name__ == '__main__':
    update_db('201908271542')
