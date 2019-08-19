#! /usr/bin/env python3

import config as cfg
import csv_parse as csv_p
import csv_clean as csv_c
import create_xml as xml_c
import get_proxy as gp
import user_input as ui

import diva_oracle_query as d_query
import gorilla_oracle_query as g_query
import merge_gor_diva_dbs as mdb

from time import localtime, strftime
from logging.handlers import TimedRotatingFileHandler

import logging
import logging.config
import os
import yaml

logger = logging.getLogger(__name__)


def set_logger():
    """Setup logging configuration
    """
    path = 'logging.yaml'

    with open(path, 'rt') as f:
        config = yaml.safe_load(f.read())
        logger = logging.config.dictConfig(config)

    return logger


def main():
    """
    This Script is specifically for migrating data from one media asset
    management system (MAM) to another (Gorilla to Dalet).
    The script calls a set of modules to execute a series of steps that
    perform the data migration: First, query the two separate dbs, then merge
    the query results based on a common field. The merged csv data is then
    parsed for rows that contain certain string patterns. The string patterns
    are specific to the data that needs to migrate. The csv created from the
    parsing is then cleaned - a metaxml field containing mediainfo is split
    out into 7 new columns, and the data from the XML elements is used to
    populate the newly created columns. The bad data from the XML is dropped,
    some incorrect data is fixed, and empty values are marked as NULL. The
    final version of the csv containing the cleaned data is then used to
    create new XMLs records to check into in the Dalet MAM.
    """

    date = str(strftime("%Y%m%d%H%M", localtime()))

    cfg.ensure_dirs()

    start_msg = f"\n\
    ================================================================\n \
                Gorilla-Diva Asset Migration Script\n\
                Version: 0.0.1\n\
                Date: August 19 2019\n\
    ================================================================\n\
    \n"

    logger.info(start_msg)

    xml_total = ui.get_user_input()

    gor_csv = g_query.buildcsv(date)
    diva_csv = d_query.buildcsv(date)
    merged_csv = mdb.pandas_merge(date, diva_csv, gor_csv)
    parsed_csv = csv_p.db_parse(date, merged_csv)
    cleaned_csv = csv_c.db_clean(date)
    # xml_c.create_xml(cleaned_csv, xml_total)
    # gp.get_proxy()

    print("="*25 + "  FINISHED  " + "="*25)


if __name__ == '__main__':
    set_logger()
    main()
