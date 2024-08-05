import logging
import os
import re
from typing import Optional, Tuple

import pandas as pd

import config as cfg
import database as db

import get_mediainfo as gmi

# Configure logger
logger = logging.getLogger(__name__)


def csv_cleann_final(date: str, cleaned_csv: Optional[str] = None) -> Tuple[str, str]:
    """
    Final Pass on the cleaned data to fix bad rows or values,  and remove any unwanted data.
    """
    config = cfg.get_config()

    root_path, db_path, csv_path = (
        config["paths"]["root_path"],
        config["paths"]["db_path"],
        config["paths"]["csv_path"],
    )

    os.chdir(csv_path)

    clean_csv_final = f"{date}_gor_diva_merged_cleaned_final.csv"
    logger.info("START FINAL PASS of GORILLA-DIVA DB CLEAN")

    df = pd.read_csv(cleaned_csv, header=0)
    df.index.name = "ROWID"
    df.to_csv(clean_csv_final)

    for index, row in df.iterrows():
        row = check_codec(row)
        row = check_framerate(row)
        row = check_resolution(row)

        df.at[index] = row

    df.to_csv(clean_csv_final)

    return logger.info("FINAL PASS COMPLETE")


def check_codec(row):
    codec_list = [
        "XAVC",
        "UHD",
        "H264",
        "MPEG2",
        "MPEG4",
        "DV",
        "DNXHD",
        "PRORES",
        "XDCAM",
        "AVC",
    ]

    if row["CODEC"] == "NULL":
        codec = gmi.get_codec(row)
        if codec != "NULL":
            logger.info(f"Found codec {codec} for {row['NAME']}")
            row["CODEC"] = codec
    if (
        row["CODEC"] == "NULL"
        and ("VM" in str(row["CONTENT_TYPE"]))
        or ("EM" in str(row["CONTENT_TYPE"]))
    ):
        logger.info("FILENAME indicates a VM or EM file, setting codec to 'PRORES'")
        row["CODEC"] = "PRORES"
    else:
        logger.error(f"Could not find codec for {row['NAME']}")

    return row


def check_framerate(row):

    framerate_list = ["23.976", "23.98", "25", "29.97", "59.94"]

    if row["TITLETYPE"] == "video" and row["FRAMERATE"] not in framerate_list:
        framerate = gmi.get_framerate(row)
        if framerate != "NULL":
            logger.info(f"Found framerate {framerate} for {row['NAME']}")
            row["FRAMERATE"] = framerate
        else:
            logger.error(f"Could not find framerate for {row['NAME']}")

    if row["TITLETYPE"] == "video" and row["FRAMERATE"] == "NULL":
        traffic_code = row["TRAFFIC_CODE"]
        matching_rows = row["TRAFFIC_CODE"] == traffic_code

        # Check if matching rows have CONTENT_TYPE containing "EM"
        em_matching_rows = matching_rows["EM" in matching_rows["CONTENT_TYPE"]]

        # Log the rows with the same TRAFFIC_CODE and CONTENT_TYPE containing "EM"
        logger.info(
            f"Matching rows for TRAFFIC_CODE {traffic_code} with CONTENT_TYPE containing 'EM':"
        )
        for _, matching_row in em_matching_rows.iterrows():
            logger.info(matching_row.to_dict())
            framerate = gmi.get_framerate(matching_row)
            if framerate != "NULL":
                logger.info(f"Found framerate {framerate} for {matching_row['NAME']}")
                row["FRAMERATE"] = framerate
            else:
                logger.error(f"Could not find framerate for {matching_row['NAME']}")

    else:
        logger.info(f"Row {row['NAME']} already has a framerate value.")

    return row


def check_resolution(row):
    if row["V_WIDTH"] == "NULL" or row["V_HEIGHT"] == "NULL":
        resolution = gmi.est_resolution(row, row["CODEC"])
        if resolution != ("NULL", "NULL"):
            logger.info(f"Found resolution {resolution} for {row['NAME']}")
            row["V_WIDTH"], row["V_HEIGHT"] = resolution
        else:
            logger.info(f"Could not find resolution for {row['NAME']}")
            if "VM" in str(row["CONTENT_TYPE"]):
                row["V_WIDTH"], row["V_HEIGHT"] = "1920", "1080"
            if "EM" in str(row["CONTENT_TYPE"]):
                row["V_WIDTH"], row["V_HEIGHT"] = "1920-est", "1080-est"
            logger.info(
                f"Setting resolution to '1920-est x 1080-est' for {row['NAME']}"
            )

    return row


if __name__ == "__main__":
    date = "202108051600"
    cleaned_csv = "202408011717_gor_diva_merged_cleaned.csv"
    csv_cleann_final(date, cleaned_csv)
