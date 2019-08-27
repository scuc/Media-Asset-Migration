#! /usr/bin/env python3

import config as cfg

import logging
import os
import sqlite3

import pandas as pd

logger = logging.getLogger(__name__)
config = cfg.get_config()


def connect(db_name='database.db'):
    """ Create a database connection."""
    try:
        conn = sqlite3.connect('database.db')
    except Exception as e:
        logger.error(e)
    return conn


def create_table(db_name, tablename, df):
    try:
        conn = sqlite3.connect(db_name)
        df.to_sql(tablename, con=connect(), if_exists='replace')
    except Exception as e:
        logger.error(e)
    return


def update_table(tablename, df):
    try:
        conn = connect()
        df.to_sql(tablename, conn, if_exists='replace', index_label='Index')
    except Exception as e:
        logger.error(e)
        print(e)

    return

def select_row(index):
    try:
        conn = connect()
        c = conn.cursor()
        sql = c.execute('''SELECT * FROM assets WHERE rowid=?''', (index,))
        row = sql.fetchone()
        print(row)
        return sql
    except Exception as e: 
        logger.error(e)
        print(e)

def update_row(tablename, index, col_name, col_val):
    try: 
        conn = connect()
        c = conn.cursor()
        sql = c.execute(''' UPDATE ?
                        SET ? = ?,
                        WHERE id = ?''', (tablename, col_name, col_val, index,))
    except Exception as e: 
        logger.error(e)
        print(e)

def fetchone_xml(guid): 
    try: 
        conn = connect()
        c = conn.cursor()
        sql = c.execute('''SELECT rowid, xml_created
                            FROM assets 
                            WHERE guid=?''', (guid,))

        xml_status = c.fetchone()
        return xml_status
    except Exception as e: 
        logger.error(e)
        print(e)


def fetchone_proxy(guid): 
    try:
        conn = connect()
        c = conn.cursor()
        sql = c.execute('''SELECT rowid, proxy_copied
                            FROM assets 
                            WHERE guid=?''', (guid,))

        proxy_status = c.fetchone()
        return proxy_status
    except Exception as e: 
        logger.error(e)
        print(e)


if __name__ == '__main__':
    fetchone_xml('00215AD34D20-8000FFFF-FFFF-F2AD-A668')
