import pyodbc
# import app_adhoc
import sys
import os
import time
import threading
from pathlib import Path
# import bintang
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0,os.path.join(PROJECT_ROOT,"bintang"))
import bintang
import logging
import yaml
from rapidfuzz import process as rfprocess
import helper as h

if __name__ == '__main__':
     # # read data from sql server
    conn_str = "DRIVER={ODBC Driver 17 for SQL Server};SERVER=localhost;PORT=1443;DATABASE=test;Trusted_Connection=yes;"
    conn = pyodbc.connect(conn_str) # the connection to the database
    # datasource paths
    path1 = r"C:\Users\60145210\Documents\Projects\bintang\test\specs.xlsx"
    
    # read excel 4,653 rps
    # to_sql 840378/2m,21sec
    print('i am here')
    #quit() 
    bt = bintang.Bintang()
    bt.create_table('specs')
    bt['specs'].read_excel(path1,'test')

    # add table to test
    bt['specs'].add_column('AddedColumn','str',50)
    # update existing column
    bt['specs'].update_column('AddedColumn','str',100 )
    bt['specs'].add_column('AnotherColumn','float')
    

    createsql = bt['specs'].gen_create_sqltable('sqlserver')
    cursor = conn.cursor()
    cursor.execute('DROP TABLE IF EXISTS specs')
    cursor.execute(createsql)
    conn.commit()
    print(createsql)
    

    colmap = {'ID':'ID','Manufacturer Name':'Manufacturer Name'}
    #colmap = ['Day','ID','Name']
    h.tic()
    bt['specs'].to_sql(conn,'dbo','specs',colmap, method='string')
    h.tac()
#     for idx, row in bt['sql_'].iterrows():
#      print(idx, row)
    conn.commit()
    conn.close()