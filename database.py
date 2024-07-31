#! /usr/bin/env python3

import logging
import os
import sqlite3

import pandas as pd

import config as cfg

logger = logging.getLogger(__name__)
config = cfg.get_config()

db_path = config["paths"]["db_path"]


def connect(db_name="database.db"):
    """Create a database connection."""
    try:
        os.chdir(db_path)
        conn = sqlite3.connect("database.db")
    except Exception as e:
        conn_err_msg = f"Error on connection to database.db"
        logger.exception(conn_err_msg)
    return conn


def create_table(db_name, tablename, df):
    try:
        conn = sqlite3.connect(db_name)
        df.to_sql(tablename, con=connect(), if_exists="replace")
    except Exception as e:
        cr_table_err_msg = f"Error on creating new db table: {tablename}"
        logger.exception(cr_table_err_msg)
    return


def update_table(tablename, df):
    try:
        conn = connect()
        df.to_sql(tablename, conn, if_exists="replace", index_label="Index")
        return
    except Exception as e:
        upd_table_err_msg = f"Error on updating the db table: {tablename}"
        logger.exception(upd_table_err_msg)


def select_row(index):
    try:
        conn = connect()
        c = conn.cursor()
        sql = """SELECT * FROM assets WHERE rowid = ?"""
        params = (index,)
        row = c.execute(sql, params).fetchone()
        conn.close()
        return row
    except Exception as e:
        sel_row_err_msg = f"Error selecting row in 'assets' at index: {index}"
        logger.exception(sel_row_err_msg)


def insert_row(index, row):
    try:
        conn = connect()
        c = conn.cursor()
        values_char = "?," * len(row)
        values = values_char[:-1]
        sql = f""" INSERT INTO assets VALUES ({values});"""
        params = row
        insert = c.execute(sql, params)
        conn.commit()
        conn.close()
        return
    except Exception as e:
        insert_row_err_msg = f"Error inserting row in 'assets' at index: {index}"
        logger.exception(insert_row_err_msg)


def update_row(tablename, index, row):
    try:
        conn = connect()
        cur = conn.cursor()
        sql = """UPDATE assets
                 SET DATATAPEID = ?,
                     AO_ID = ?, 
                     AO_UUID = ?, 
                     AO_COMMENT= ?, 
                     AO_CATEGORY = ?, 
                     AO_DATE_ARCHIVE = ?, 
                     AO_LAST_READ = ?, 
                     AO_OBJECT_SIZE = ?, 
                     OC_COMPONENT_NAME = ?, 
                     OC_COMPONENT_IS_DELETED = ?, 
                     ON_CATEGORY = ?, 
                     ON_MEDIA_NAME = ?, 
                     ON_DATE_CREATION = ?, 
                     ON_LAST_ACCESS_TIME = ?, 
                     CH_CHECKSUM_VALUE = ?, 
                     CH_CHECKSUM_DATE = ?, 
                     CY_CHECKSUM_TYPE = ?, 
                     _merge = ?
                 WHERE GUID = ?"""
        params = (
            row[4],
            row[24],
            row[25],
            row[26],
            row[27],
            row[28],
            row[29],
            row[30],
            row[31],
            row[32],
            row[33],
            row[34],
            row[35],
            row[36],
            row[37],
            row[38],
            row[39],
            row[40],
            row[1],
        )

        cur.execute(sql, params)
        conn.commit()
        conn.close()
        return
    except Exception as e:
        upd_row_err_msg = f"Error updating row in {tablename} at index: {index}"
        logger.exception(upd_row_err_msg)


def update_column(tablename, col_name, col_value, index):
    try:
        conn = connect()
        cur = conn.cursor()
        sql = (
            f"""UPDATE {tablename} SET {col_name} = {col_value} WHERE rowid = {index}"""
        )
        params = (tablename, col_name, col_value, index)
        cur.execute(sql)
        conn.commit()
        conn.close()
        return
    except Exception as e:
        upd_row_err_msg = f"Error updating row in {tablename} at index: {index}"
        logger.exception(upd_row_err_msg)


def drop_row(tablename, index, guid):
    try:
        conn = connect()
        cur = conn.cursor()
        sql = f"""DELETE FROM assets WHERE (rowid = ? AND guid = ?)"""
        params = (index, guid)
        cur.execute(sql, params)
        conn.commit()
        conn.close()
        return
    except Exception as e:
        fetchxml_err_msg = (
            f"Error dropping row in {tablename} at index: {index} for guid: {guid}"
        )
        logger.exception(fetchxml_err_msg)
        logger.exception(e)


def fetchall(tablename):
    try:
        conn = connect()
        cur = conn.cursor()
        sql = f"""SELECT * FROM {tablename}"""
        params = (tablename,)
        rows = cur.execute(sql).fetchall()
        conn.close()
        return rows
    except Exception as e:
        fetchall_err_msg = f"Error on fetching rows for db table: {tablename}"
        logger.exception(fetchall_err_msg)


def fetchall_proxy(tablename):
    try:
        conn = connect()
        cur = conn.cursor()
        sql = f"""SELECT * FROM {tablename} WHERE (TITLETYPE is 'video' AND PROXY_COPIED = 0)"""
        params = (tablename,)
        rows = cur.execute(sql).fetchall()
        conn.close()
        return rows
    except Exception as e:
        fetchall_err_msg = f"Error on fetching rows for db table: {tablename}"
        logger.exception(fetchall_err_msg)


def fetchone_guid(guid):
    try:
        conn = connect()
        cur = conn.cursor()
        sql = """SELECT rowid FROM assets WHERE guid = ?"""
        params = (guid,)
        guid_status = cur.execute(sql, params).fetchone()
        conn.close()
        print(guid_status)
        return guid_status
    except Exception as e:
        fetchxml_err_msg = f"Error on fetching xml status from db for guid: {guid}"
        logger.exception(fetchxml_err_msg)


def fetchone_xml(guid):
    try:
        conn = connect()
        cur = conn.cursor()
        sql = """SELECT rowid, xml_created FROM assets WHERE guid = ?"""
        params = (guid,)
        xml_status = cur.execute(sql, params).fetchone()
        print(
            f"============= XML STATUS ON THE DB FUNC = {xml_status}  =================="
        )
        conn.close()
        return xml_status
    except Exception as e:
        fetchxml_err_msg = f"Error on fetching xml status from db for guid: {guid}"
        logger.exception(fetchxml_err_msg)


def fetchone_proxy(guid):
    try:
        conn = connect()
        cur = conn.cursor()
        sql = """SELECT rowid, proxy_copied FROM assets WHERE guid = ?"""
        params = (guid,)
        proxy_status = cur.execute(sql, params).fetchone()
        conn.close()
        return proxy_status
    except Exception as e:
        fetchprxy_err_msg = f"Error on fetching proxy status from db for guid: {guid}"
        logger.exception(fetchprxy_err_msg)


if __name__ == "__main__":
    fetchone_guid("00215AD34D20-8000FFFF-FFFF-C2F5-C5E0")
