# Media-Asset-Migration
A script for migrating data from one media asset management (MAM) system to another.

## Description
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

## Prerequisites 

* Python 3.6 or higher
* [pandas](https://pandas.pydata.org) 
* [cx_Oracle](https://oracle.github.io/python-cx_Oracle/)

## Files Included

* `main.py`
* `config.py`
* `gorilla_oracle_query.py`
* `diva_oracle_query.py`
* `merge_gor_diva_dbs.py`
* `csv_parse.py`
* `csv_clean.py`
* `create_xml.py`
* `logging.yaml `


## Getting Started

* Install prerequisities 
* Create a `config.yaml` document with the format: 
&nbsp;   &nbsp;   &nbsp;   &nbsp;   &nbsp;  

&nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; paths: &nbsp;   &nbsp;   &nbsp;   &nbsp;   &nbsp;  
&nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; rootpath:&nbsp;   &nbsp;   &nbsp;   &nbsp;   &nbsp;  

&nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; oracle-db-gor:&nbsp;   &nbsp;   &nbsp;   &nbsp;   &nbsp;  
  &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; user:&nbsp;   &nbsp;   &nbsp;   &nbsp;   &nbsp;  
  &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; pass:&nbsp;   &nbsp;   &nbsp;   &nbsp;   &nbsp;  
  &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; url: &nbsp;   &nbsp;   &nbsp;   &nbsp;   &nbsp;  

&nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; oracle-db-diva: &nbsp;   &nbsp;   &nbsp;   &nbsp;   &nbsp;  
  &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; user:  &nbsp;   &nbsp;   &nbsp;   &nbsp;   &nbsp;  
  &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; pass:  &nbsp;   &nbsp;   &nbsp;   &nbsp;   &nbsp;  
  &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; url:  &nbsp;   &nbsp;   &nbsp;   &nbsp;   &nbsp;  

