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
    
    bt = bintang.Bintang('my bintang')
    bt.create_table('Person')
    person = bt.get_table('Person')

    person.insert([1,'John','Smith','1 Station St'], ['id','name','surname','address'])
    person.insert([2,'Jane','Brown','Digging','8 Parade Rd'], ['id','name','surname','address'])
    person.insert([3,'Okie','Dokey','7 Ocean Rd'], ['id','name','surname','address'])
    person.insert([4,'Maria','Digging'], ['id','name','hobby'])
    person.insert([5,'Bing','Digging'], ['id','name','hobby'])

    # print records
    person.print()
    
    # let map column ID, FirstName, LastName, Address
    columns = {'ID':'id', 'FirstName':'name', 'LastName':'surname', 'Address':'address'}
    #print(dict(zip(columns.values(),columns.keys())))
    # connect to database
    conn = pyodbc.connect("DRIVER={ODBC Driver 17 for SQL Server};SERVER=localhost;PORT=1443;DATABASE=test;Trusted_Connection=yes;")  
    # print(conn.getinfo(pyodbc.SQL_DBMS_NAME))
    # print(str(conn))
    # print(type(conn))
    # print(str(type(conn)) == "<class 'pyodbc.Connection'>")
    # #print(isinstance(conn,pyodbc.Connection))
    # quit()
    # print(conn.)

    # quit()
    
    # for k,v in conn.__items__():
    #     print(k,v)
    # quit()

    # send data to sql
    #ret = person.to_sql(conn, 'Person', columns, method='prep')
    ret = person.to_sql(conn, 'Person', columns, schema='dbo', method='prep')
    print(f'{ret} record(s) affected.')
    conn.commit()
    conn.close()
    