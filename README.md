# Media-Asset-Migration
A script for migrating data from one media asset management (MAM) system to another.

## Description
The script takes user input and calls a set of modules to perform a data migration 
from one MAM system to another. In this script data is moved from [Gorilla EFCS](https://www.gorilla-technology.com/) and [DIVAArchive](https://www.goecodigital.com) to the [Dalet Galaxy](https://www.dalet.com/) MAM. 
An SQLite DB is used to store the metadata and the migration status for each asset. 

The script follows the a series of steps: 


1. Query the two separate dbs (Gorilla and DIVA), export the data to two seperate CSV files.
2. Use Pandas to merge the query results based on a common field, and export to a new csv.
3. Parse the merged data for rows that contain certain string patterns. </br>
	The string patterns are specific to the data that needs to migrate. </br>
	Export a new csv of the parsed data. 
4. Clean the parsed data. A metaxml field containing mediainfo is split
	out into 7 new columns, </br> and the data from the XML elements is used to
	populate the newly created columns. </br>
	The bad data from the XML is dropped, some incorrect data is fixed, and empty values are marked as NULL. </br> 
	If metadata is missing, a best-guess is attempted based on the filename information. </br> 
	If that is not possible or unsucessful, the field is marked Null. 
	After the info is split out into seperate columns, </br>
	the original metaxml column is dropped, and the data is moved into a SQL DB. 
5. There is an optional step to crosscheck all exported data against the information in the DB, </br>
	to ensure assets are not migrated more than once. 
6. Based on the user input, a number of XMLs are exported from the DB. 
7. Optional step to also export the corresponding proxy file along with the XML. </br>
	Proxies are exported only for assets with the titletype = 'video'. </br>
	Assets with the titletype = 'archive' assets do not have a proxy to export. 
 


## Prerequisites 

* Python 3.6 or higher
* [pandas](https://pandas.pydata.org) 
* [cx_Oracle](https://oracle.github.io/python-cx_Oracle/)
* [SQLite](https://www.sqlite.org/download.html)

## Files Included

* `main.py`
* `user_input.py`
* `config.py`
* `gorilla_oracle_query.py`
* `diva_oracle_query.py`
* `merge_dbs.py`
* `csv_parse.py`
* `csv_clean.py`
* `crosscheck_assets.py`
* `update_db.py`
* `create_xml.py`
* `get_proxy.py`
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

