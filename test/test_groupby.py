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
from thefuzz import process as thefuzzprocess
import helper as h

if __name__ == '__main__':
     # # read data from sql server
    conn_str = "DRIVER={ODBC Driver 17 for SQL Server};SERVER=EHL5CD8434KLM;PORT=1443;DATABASE=test;Trusted_Connection=yes;"
    conn = pyodbc.connect(conn_str) # the connection to the database
    # datasource paths
    path1 = r"C:\Users\60145210\Documents\Projects\bintang\test\specs.xlsx"
    #path2 = r"C:\Users\60145210\Documents\AFMO Data\20230316 MNCLHD CHBH Intercom\T\ref\cstTI - Manufacturer - Training - src_manuf out.xlsx"
    # read excel 4,653 rps
    # to_sql 840378/2m,21sec

    
    bt = bintang.Bintang()
    h.tic()
    bt.read_excel(path1,'specs')
    h.tac()
    res = bt['specs'].groupby(['Manufacturer Name'], 'groupby_tab',count=True)
  
    for idx, row in bt['groupby_tab'].iterrows():
        print(idx, row)
    
    #res2 = res.groupby_count(['Manufacturer Name'],'mycount')
    
    path = Path(__file__).resolve().parent / 'specs_groupby.xlsx'
    bt['groupby_tab'].to_excel(path)    

    