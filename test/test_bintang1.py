import os
import sys
from pathlib import Path
import json
import pyodbc


use_package = False
if use_package is False:
    # definec project root
    PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
    # add code directory
    sys.path.insert(0,os.path.join(PROJECT_ROOT,"bintang"))

    
import bintang


if __name__ == "__main__":
    bt = bintang.Bintang("my bintang")
    
    bt.create_table("Product")
    prod = bt['Product']
    prod.insert({'id':1, 'name':'Hook','price':1.60})
    prod.insert({'id':2, 'name':'Sinker','price':1.20})
    prod.insert({'id':3, 'name':'Reels','price':75})

    dic = {'key':5}
    prod.insert({'id':5, 'name':json.dumps(dic)})
    prod.insert([6,False], ['id','test'])
    
    for idx, row in prod.iterrows():
        print(idx, row)

    # connect to database
    conn = pyodbc.connect("DRIVER={ODBC Driver 17 for SQL Server};SERVER=localhost;PORT=1443;DATABASE=test;Trusted_Connection=yes;")  
    
    createsql = bt['product'].gen_create_sqltable('sqlserver')
    cursor = conn.cursor()
    cursor.execute('DROP TABLE IF EXISTS product')
    cursor.execute(createsql)
    conn.commit()    

    # let map column ID, FirstName, LastName, Address
    columns = {'id':'id', 'name':'name', 'price':'price', 'test':'test'}
    
    # send data to sql
    ret = prod.to_sql(conn, 'dbo', 'Product', columns) #, method='string'
    conn.commit()
    print(f'{ret} record(s) affected.')

