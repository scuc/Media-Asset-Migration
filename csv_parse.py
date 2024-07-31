#! /usr/bin/env python3

import csv
import logging
import os
import re

import pandas as pd
import yaml

import config as cfg

logger = logging.getLogger(__name__)


def db_parse(date, merged_csv):
    """
    Loop through the gor-diva merged csv and parse out rows for specific file types.
    write out the rows to a pandas df, then save to a new csv.
    """

    config = cfg.get_config()

    root_path = config["paths"]["root_path"]
    csv_path = config["paths"]["csv_path"]

    os.chdir(csv_path)

    parsed_csv = date + "_" + "gor_diva_merged_parsed.csv"

    index_count = 0
    parsed_count = 0

    try:
        with open(merged_csv, mode="r", encoding="utf-8-sig") as m_csv, open(
            parsed_csv, "w+", newline=""
        ) as p_csv:

            parse_1_msg = f"START GORILLA-DIVA DB PARSE"
            logger.info(parse_1_msg)
            print(parse_1_msg)

            pd_reader = pd.read_csv(m_csv, header=0)
            df = pd.DataFrame(pd_reader)

            for index, row in df.iterrows():

                name = str(row["NAME"]).upper()
                print(str(index_count) + "    " + name)

                """
                index_count value set to 200K, arbitrary number, the value should 
                be set higher as the DB grows.

                """
                """
                1)  ^: This asserts the start of the string, ensuring that the pattern matches from the beginning.

                2)  (?!.*(PGS|SVM|SDM)): This is a negative lookahead assertion (?!...) which ensures that the string does not contain the substrings "PGS", "SVM", or "SDM" anywhere. If any of these substrings are found, the match fails.

                3)  ((?<![A-Z])|(?<=(-|_))): This part is a capturing group ( ... ) containing two alternatives joined by a logical OR |.

                4)  (?<![A-Z]): This is a negative lookbehind assertion (?<!...) which ensures that the match is not preceded by an uppercase letter.

                5)  (?<=(-|_)): This is a positive lookbehind assertion (?<=...) which ensures that the match is preceded by a hyphen or underscore.

                6)  VM|EM|AVP|PPRO|FCP|PTS|GRFX|GFX|UHD|XDCAM|XDCAMHD|WAV|WAVS: This is a capturing group containing a list of options separated by the pipe |. It matches one of the specified options: "VM", "EM", "AVP", "PPRO", "FCP", "PTS", "GRFX", "GFX", "UHD", "XDCAM", "XDCAMHD", "WAV", or "WAVS".

                7)  (?=(-|_|[1-5])?): This is a positive lookahead assertion (?=...) which ensures that the match is followed by an optional hyphen, underscore, or a digit between 1 and 5.

                8)  (?![A-Z]): This is a negative lookahead assertion (?!...) which ensures that the match is not followed by an uppercase letter.
                --------------------------------------
                the regex pattern ensures that:

                The string does not contain "PGS", "SVM", or "SDM".
                The matched substring starts either not with a digit or an uppercase letter, or with a hyphen or underscore.
                The matched substring is one of the specified options.
                The matched substring is optionally followed by a hyphen, underscore, or a digit between 1 and 5.
                The matched substring is not followed by an uppercase letter.
                """

                if index_count <= 200000:

                    # em_check = re.search(
                    #     r"((?<![0-9]|[A-Z])|(?<=(-|_)))(VM|EM|AVP|PPRO|FCP|PTS|GRFX|GFX|UHD|XDCAM|XDCAMHD)(?=(-|_|[1-5])?)(?![A-Z])",
                    #     name,
                    # )
                    em_check = re.search(
                        r"^(?!.*(?:PGS|SVM|SDM|CEM|PROMO))[-_]*(VM|EM|AVP|PPRO|FCP|PTS|GRFX|GFX|UHD|XDCAM|XDCAMHD|WAV|WAVS)[-_]*$",
                        name,
                    )

                    qc_check = re.search(r"(?<=-|_)OUTGOING(?=[QC]?|-|_)", name)

                    if em_check is not None and qc_check is None and parsed_count == 0:
                        dft = pd.DataFrame(row).transpose()
                        dft.to_csv(p_csv, mode="a", index=False, header=True)
                        parsed_count += 1
                    elif (
                        em_check is not None and qc_check is None and parsed_count != 0
                    ):
                        dft = pd.DataFrame(row).transpose()
                        dft.to_csv(p_csv, mode="a", index=False, header=False)
                        parsed_count += 1
                    else:
                        parse_2_msg = f"{name} removed from the dataset"
                        logger.info(parse_2_msg)
                        pass

                    index_count += 1

            m_csv.close()
            p_csv.close()

            os.chdir(root_path)

            parse_3_msg = f"GORILLA-DIVA DB PARSE COMPLETE"
            logger.info(parse_3_msg)
            print(parse_3_msg)

        return parsed_csv

    except Exception as e:
        db_parse_excp_msg = f"\n\
        Exception raised on the Gor-Diva DB Parse.\n\
        Error Message:  {str(e)} \n\
        "

        logger.exception(db_parse_excp_msg)

        print(db_parse_excp_msg)


if __name__ == "__main__":
    db_parse()
