import pyodbc
# import app_adhoc
import sys
import os
import time
import threading
from pathlib import Path
# import bintang
# PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
# sys.path.insert(0,os.path.join(PROJECT_ROOT,"bintang"))
import bintang
import logging
import yaml
#from thefuzz import process as thefuzzprocess
import helper as h

if __name__ == '__main__':
     # # read data from sql server
    conn_str = "DRIVER={ODBC Driver 17 for SQL Server};SERVER=localhost;PORT=1443;DATABASE=test;Trusted_Connection=yes;"
              #"DRIVER={ODBC Driver 17 for SQL Server};SERVER=localhost;PORT=1443;DATABASE=test;Trusted_Connection=yes;"  
    conn = pyodbc.connect(conn_str) # the connection to the database
    
    sql_str = 'SELECT * FROM INFORMATION_SCHEMA.TABLES'
    bt = bintang.Bintang()
    bt.create_table('tables')
    bt['tables'].read_sql(conn,sql_str)
    bt['tables'].print()