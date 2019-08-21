#! /usr/bin/env python3

import csv
import logging
import os
import re
import yaml

import config as cfg
import pandas as pd


logger = logging.getLogger(__name__)


def db_parse(date, merged_csv):
    """
    Loop through the gor-diva merged csv and parse out rows for specific file types. write out the rows to a pandas df, then save to a new csv.
    """

    config = cfg.get_config()

    rootpath = config['paths']['rootpath']

    os.chdir(rootpath + "_CSV_Exports")

    parsed_csv = (date + "_" + "gor_diva_merged_parsed.csv")

    index_count = 0
    parsed_count = 0

    try:
        with open(merged_csv, mode='r', encoding='utf-8-sig') as m_csv, open(parsed_csv, 'w+', newline='') as p_csv:

            parse_1_msg = f"START GORILLA-DIVA DB PARSE"
            logger.info(parse_1_msg)
            print(parse_1_msg)


            pd_reader = pd.read_csv(m_csv, header=0)
            df = pd.DataFrame(pd_reader)

            for index, row in df.iterrows():

                name = str(row['NAME']).upper()
                print(str(index_count) + "    " + name)

                if index_count <= 180000:

                    em_check = re.search(
                        r'((?<![0-9]|[A-Z])|(?<=(-|_)))(VM|EM|AVP|PPRO|FCP|PTS|AVP|GRFX|GFX|UHD)(?=(-|_)?)(?![0-9]|[A-Z])', name)
                    qc_check = re.search(r'(?<=-|_)OUTGOING(?=[QC]?|-|_)', name)

                    if (em_check is not None 
                        and qc_check is None
                        and parsed_count == 0):
                        dft = pd.DataFrame(row).transpose()
                        dft.to_csv(p_csv, mode='a', index=False, header=True)
                        parsed_count += 1
                    elif (em_check is not None 
                          and qc_check is None
                          and parsed_count != 0):
                        dft = pd.DataFrame(row).transpose()
                        dft.to_csv(p_csv, mode='a', index=False, header=False)
                        parsed_count += 1
                    else:
                        pass

                    index_count +=1

            m_csv.close()
            p_csv.close()

            os.chdir(rootpath)

            parse_2_msg = f"GORILLA-DIVA DB PARSE COMPLETE"
            logger.info(parse_2_msg)
            print(parse_2_msg)

        return parsed_csv

    except Exception as e:
        db_parse_excp_msg = f"\n\
        Exception raised on the Gor-Diva DB Parse.\n\
        Error Message:  {str(e)} \n\
        "

        logger.exception(db_parse_excp_msg)

        print(db_parse_excp_msg)


# if __name__ == '__main__':
#     db_parse()
