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
    except Error as e:
        logger.error(e)
    return conn


def create_table(db_name, table_name, df):
    try:
        conn = sqlite3.connect(db_name)
        df.to_sql(table_name, con=connect(), if_exists='replace')
    except Error as e:
        logger.error(e)
    return


def update_table(table_name, df):
    try:
        conn = connect()
        df.to_sql(table_name, conn, if_exists='replace', index_label='Index')
    except Error as e:
        # logger.error(e)
        print(e)

    return

def select_row(index):
    conn = connect()
    c = conn.cursor()
    sql = c.execute('''SELECT * FROM assets WHERE rowid=?''', (index,))
    row = sql.fetchone()
    print(row)
    return sql

def update_row(table_name, index, col_name, col_val):
    conn = connect()
    c = conn.cursor()
    sql = c.execute(''' UPDATE ?
                    SET ? = ? ,,
                    WHERE id = ?''', (table_name, col_name, col_val,index,))
    print


def fetchone_xml(guid): 
    conn = connect()
    c = conn.cursor()
    sql = c.execute("""SELECT rowid, xml_created
                         FROM assets 
                         WHERE guid=?""", (guid,))

    xml_status = c.fetchone()
    return xml_status


def fetchone_proxy(guid): 
    conn = connect()
    c = conn.cursor()
    sql = c.execute("""SELECT rowid, proxy_copied
                         FROM assets 
                         WHERE guid=?""", (guid,))

    proxy_status = c.fetchone()
    return proxy_status


if __name__ == '__main__':
    fetchone_xml('00215AD34D20-8000FFFF-FFFF-F2AD-A668')
