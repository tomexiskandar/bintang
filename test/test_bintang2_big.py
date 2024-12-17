import os
import sys
from pathlib import Path
import json
import pyodbc
import helper as h


use_package = False
if use_package is False:
    # definec project root
    PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
    # add code directory
    sys.path.insert(0,os.path.join(PROJECT_ROOT,"bintang"))

    
import bintang
from bintang.log import log


if __name__ == "__main__":
    bt = bintang.Bintang("my bintang")
    
    bt.create_table("Person")
    print('hello in main')

    print(bt)

    
    person = bt.get_table("Person")
    log.debug(h.tic())
    for _ in range(1000000):
        person.insert((1,"John",35,"1 Station St"),("id","name","age","address"))
        person.insert([2,"Jane",17,"Digging","8 Parade Rd"],("id","name","age","hobby","address"))
        person.insert((3,"Nutmeg","Fishing",'7 Ocean Rd'), ("id","name","hobby","Address"))
        person.insert((4,"Maria","Digging",None), ("id","name","hobby","Address"))
        person.insert((5,"Bing","Digging",None), ("id","name","hobby","Address"))
    log.debug(f'insert time: {h.tac()}')

    log.debug(h.tic())
    for idx, row in person.iterrows():
        val = row['name']
    log.debug(f'iterrows time: {h.tac()}')

    # connect to database
    conn = pyodbc.connect("DRIVER={ODBC Driver 17 for SQL Server};SERVER=localhost;PORT=1443;DATABASE=test;Trusted_Connection=yes;")  
    
    createsql = bt['Person'].gen_create_sqltable('sqlserver')
    cursor = conn.cursor()
    cursor.execute('DROP TABLE IF EXISTS Person')
    cursor.execute(createsql)
    conn.commit()    

    # let map column ID, FirstName, LastName, Address
    columns = person.get_columns()
    
    # send data to sql
    # log.debug(h.tic())
    # ret = person.to_sql(conn, 'dbo', 'Person', columns, method='prep', max_rows = 300) #, method='string'
    # conn.commit()
    # log.debug(f'to_sql time: {h.tac()}')
    # print(f'{ret} record(s) affected.')

