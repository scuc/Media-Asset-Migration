import logging
import os
from typing import Optional, Tuple

import pandas as pd

import config as cfg
import get_mediainfo as gmi

# Configure logger
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


def csv_clean_final(date: str, cleaned_csv: Optional[str] = None) -> Tuple[str, str]:
    """
    Final Pass on the cleaned data to fix bad rows or values, and remove any unwanted data.
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

    for index, row in df.iterrows():
        original_row = row.copy()

        if row["TITLETYPE"] != "video":
            continue

        row = check_codec(row)
        row = check_framerate(row, df)
        row = check_resolution(row)

        # Log changes
        log_changes(original_row, row, index)

        # Update specific columns in the DataFrame
        df.loc[index, ["CODEC", "FRAMERATE", "V_WIDTH", "V_HEIGHT"]] = row[
            ["CODEC", "FRAMERATE", "V_WIDTH", "V_HEIGHT"]
        ]

    df.to_csv(clean_csv_final, index=False)

    logger.info("FINAL PASS COMPLETE")
    return clean_csv_final, os.path.join(csv_path, clean_csv_final)


def log_changes(original_row, updated_row, index):
    changes = []
    for col in original_row.index:
        if original_row[col] != updated_row[col]:
            changes.append(f"{col}: {original_row[col]} -> {updated_row[col]}")
    if changes:
        logger.info(f"Changes for row {index}:\n" + "\n".join(changes))


def check_codec(row):
    logger.info(f"Checking codec for {row['NAME']}")
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

    if row["CODEC"] != "NULL" and not pd.isnull(row["CODEC"]):
        return row

    codec = gmi.get_codec(row)
    if codec[1] != "NULL":
        logger.info(f"Found codec {codec[1]} for {row['NAME']}")
        row["CODEC"] = codec
    elif "VM" in str(row["CONTENT_TYPE"]) or "EM" in str(row["CONTENT_TYPE"]):
        logger.info("FILENAME indicates a VM or EM file, setting codec to 'PRORES'")
        row["CODEC"] = "PRORES"
    else:
        logger.error(f"Could not find codec for {row['NAME']}")

    return row


def check_framerate(row, df):
    logger.info(f"Checking framerate for {row['NAME']}")
    framerate_list = ["23.976", "23.98", "25", "29.97", "59.94"]

    if row["TITLETYPE"] != "video" or row["FRAMERATE"] in framerate_list:
        return row

    if row["FRAMERATE"] == "NULL" or pd.isnull(row["FRAMERATE"]):
        traffic_code = row["TRAFFIC_CODE"]
        matching_rows = df[df["TRAFFIC_CODE"] == traffic_code]

        em_matching_rows = matching_rows[
            matching_rows["CONTENT_TYPE"].str.contains("EM", na=False)
        ]

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

    return row


def check_resolution(row):
    logger.info(f"Checking resolution for {row['NAME']}")

    if (
        row["V_WIDTH"] != "NULL"
        and row["V_HEIGHT"] != "NULL"
        and not pd.isnull(row["V_WIDTH"])
        and not pd.isnull(row["V_HEIGHT"])
    ):
        return row

    else:
        logger.info(f"No resolution for {row['NAME']}, using best estimate.")
        if (
            "UHD" in str(row["CONTENT_TYPE"])
            or "HDR" in str(row["NAME"])
            or "XAVC" in str(row["CODEC"])
        ):
            row["V_WIDTH"], row["V_HEIGHT"] = "3840", "2160"
            logger.info(
                f"Setting (estimated) resolution to '3840 x 2160' for {row['NAME']}"
            )
        elif (
            "VM" in str(row["CONTENT_TYPE"])
            or "EM" in str(row["CONTENT_TYPE"])
            and "UHD" not in str(row["CONTENT_TYPE"])
        ):
            row["V_WIDTH"], row["V_HEIGHT"] = "1920", "1080"

        logger.info(
            f"Setting (estimated) resolution to '1920 x 1080' for {row['NAME']}"
        )

    return row


if __name__ == "__main__":
    date = "202108051600"
    cleaned_csv = "202408011717_gor_diva_merged_cleaned.csv"
    csv_clean_final(date, cleaned_csv)
