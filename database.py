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
        conn_err_msg = (f"Error on connection to database.db")
        logger.exception(conn_err_msg)
        logger.exception(e)
    return conn


def create_table(db_name, tablename, df):
    try:
        conn = sqlite3.connect(db_name)
        df.to_sql(tablename, con=connect(), if_exists='replace')
    except Exception as e:
        cr_table_err_msg = f"Error on creating new db table: {tablename}"
        logger.exception(cr_table_err_msg)
        logger.exception(e)
    return


def update_table(tablename, df):
    try:
        conn = connect()
        df.to_sql(tablename, conn, if_exists='replace', index_label='Index')
        return
    except Exception as e:
        upd_table_err_msg = f"Error on updating the db table: {tablename}"
        logger.exception(e)


def select_row(index):
    try:
        conn = connect()
        c = conn.cursor()
        sql = '''SELECT * FROM assets WHERE rowid = ?'''
        params = (index,)
        row = c.execute(sql, params).fetchone()
        conn.close()
        return row
    except Exception as e: 
        sel_row_err_msg = f"Error selecting row in 'assets' at index: {index}"
        logger.exception(sel_row_err_msg)
        logger.exception(e)


def update_row(tablename, col_name, col_value, index):
    try: 
        conn = connect()
        cur = conn.cursor()
        sql = f'''UPDATE {tablename} SET {col_name} = {col_value} WHERE rowid = {index}'''
        params = (tablename, col_name, col_value, index)
        cur.execute(sql)
        conn.commit()
        conn.close()
        return
    except Exception as e:
        upd_row_err_msg = f"Error updating row in {tablename} at index: {index}" 
        logger.exception(upd_row_err_msg)
        logger.exception(e)


def fetchone_xml(guid): 
    try: 
        conn = connect()
        cur = conn.cursor()
        sql = '''SELECT rowid, xml_created FROM assets WHERE guid = ?'''
        params = (guid,)
        xml_status = cur.execute(sql, params).fetchone()
        conn.close()
        return xml_status
    except Exception as e: 
        fetchxml_err_msg = f"Error on fetching xml status from db for guid: {guid}"
        logger.exception(fetchxml_err_msg)
        logger.exception(e)


def fetchone_proxy(guid): 
    try:
        conn = connect()
        cur = conn.cursor()
        sql = '''SELECT rowid, proxy_copied FROM assets WHERE guid = ?'''
        params = (guid,)
        proxy_status = cur.execute(sql, params).fetchone()
        conn.close()
        return proxy_status
    except Exception as e: 
        fetchprxy_err_msg = f"Error on fetching proxy status from db for guid: {guid}"
        logger.exception(fetchprxy_err_msg)
        logger.exception(e)


if __name__ == '__main__':
    update_row('assets','xml_created', 0, 0)
