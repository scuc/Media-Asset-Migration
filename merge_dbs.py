#! /usr/bin/env python3

import logging
import os

import config as cfg
import pandas as pd

from time import localtime, strftime

logger = logging.getLogger(__name__)


def pandas_merge(date, diva_csv, gor_csv):
    """
    Creates a merged CSV from Oracle DBs of the Gorilla MAM and DivaArchive. 
    The merge is performed by converting the CSVs to pandas dataframes and using a common key.
    """
    
    config = cfg.get_config()
    rootpath = config['paths']['rootpath']
    csvpath = config['paths']['csvpath']

    try:
        os.chdir(csvpath)

        gor_source = str(gor_csv)
        div_source = str(diva_csv)

        gor_reader = pd.read_csv(gor_source)
        div_reader = pd.read_csv(div_source)

        merged_csv = (date + "_" + "gor_diva_merged_export.csv")

        merge_1_msg = f"START GORILLA-DIVA DB MERGE"
        logger.info(merge_1_msg)

        with open(merged_csv, mode='w+', encoding='utf-8-sig') as m_csv:

            merged_df = gor_reader.merge(div_reader, on='GUID')

            merged_df = pd.merge(gor_reader, div_reader, left_on=["GUID"], right_on=["GUID"], how='outer',
                                 indicator=True)

            left_count = merged_df.loc[merged_df._merge == 'left_only', '_merge'].count()

            right_count = merged_df.loc[merged_df._merge == 'right_only', '_merge'].count()

            both_count = merged_df.loc[merged_df._merge == 'both', '_merge'].count()

            merged_df.to_csv(m_csv, mode='a', index=False, header=True)

            # m_count = merged_df.shape[0]
            # merged_dd = merged.drop_duplicates(subset="GUID", inplace=True)
            # dd_count = merged_dd.shape[0]
            # merged_dd.to_csv(m_csv, mode='a', index=False, header=True)

        m_csv.close()

        merge_2_msg = f"\n\
        ==================================================================\n\
                            Gor-DIVA DB MERGE  Complete \n\
                    {str(strftime('%A, %d. %B %Y %I:%M%p', localtime()))} \n\
                    Rows Merged:    {str(both_count)}\n\
                    Unmerged Gorilla Objects:    {str(left_count)}\n\
                    Unmerged Diva Objects:    {str(right_count)}\n\
        ==================================================================\
        "

        logger.info(merge_2_msg)

        os.chdir(rootpath)

        return merged_csv

    except Exception as e:
        db_merge_excp_msg = f"\n\
        Exception raised on the Gor-Diva DB Merge.\n\
        Error Message:  {str(e)} \n\
        "

        logger.exception(db_merge_excp_msg)

        print(db_merge_excp_msg)

# if __name__ == '__main__':
#     pandas_merge()
