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
    cursor = conn.cursor()
    sql_str = "SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE 'abc%' AND LEN(TABLE_NAME)>3"
    bt = bintang.Bintang()
    bt.create_table('tables')
    bt['tables'].read_sql(conn, sql_str)
    bt['tables'].print()
    sql_str_drop = 'DROP TABLE '
    for idx, row in bt['tables'].iterrows():
        print(row['TABLE_NAME'])
        cursor.execute('DROP TABLE {};'.format(row['TABLE_NAME']))
    conn.commit()    
    conn.close()

    # connect to sql server
#     conn_str = "DRIVER={ODBC Driver 17 for SQL Server};SERVER=localhost;PORT=1443;DATABASE=test;Trusted_Connection=yes;"
#     conn = pyodbc.connect(conn_str)
#     sql_str = "SELECT * FROM Person"
#     params = ('Dokie')
#     bt = bintang.Bintang()
#     bt.create_table('Person')
#     bt['Person'].read_sql(conn, sql_str)
#     for idx, row in bt['Person'].iterrows():
#         print(idx, row)
#     # would print {'ID': 3, 'FirstName': 'Okie', 'LastName': 'Dokey', 'address': '7 Ocean Rd'}

#     conn.close() 
#     bt['Person'].print()