import os
import sys
from pathlib import Path
import json
import pyodbc

import bintang


 ### NOT WORKING AT THE MOMENT BECAUSE DRIVER CANNOT BE INTALLED DUE TO MS DEFENDER   
import psycopg
from psycopg.rows import dict_row
from psycopg.types import TypeInfo

if __name__ == "__main__":
    bt = bintang.Bintang("test_postgresql")
    bt.create_table("Product")
    prod = bt['Product']
    # prod.insert({'id':1, 'brand': 'Shimano', 'class':'rod', 'name':'Extraction','price':299})
    # prod.insert({'id':2, 'brand': 'Shimano', 'class':'rod', 'name':'Zodias Travel','price':399})
    # prod.insert({'id':3, 'brand': 'Ugly Stik', 'class':'rod', 'name':'Balance II','price':63.99})
    # prod.insert({'id':4, 'brand': 'Shimano', 'class':'rod', 'name':'Zodias Travel','price':399})
  
    conn = psycopg.connect(dbname="testdb", user="userdb", password="password", host="localhost", port=5432)#, row_factory=dict_row)
    cursor = conn.cursor()
    cursor.execute("select version()")
    data = cursor.fetchone()
    print("connection establish to:", data)

    # create sql table
    # createsql = prod.gen_create_sqltable('postgresql')
    # print(createsql)
    
    # db_table = prod.name
    # cursor.execute(f'DROP TABLE IF EXISTS {db_table}')
    # cursor.execute(createsql)

    ### read data
    columns = ['id','brand','class','name','price']
    on =[('id','id'),('brand','brand'),('class','class'),('name','name')]
    prod.to_sql_upsert_dev(conn, "public", "Product", columns, on)
    
    
    

    


    # columns = prod.get_columns()
    # ret = prod.to_sql(conn, 'public', 'product', columns, method='string' ) #inject
    # # ret = prod.to_sql(conn, 'public', 'product', columns, method='string' )
    # #ret = prod.to_sql(conn, 'public', 'product', columns, method='prep')
    # print(f'{ret} record(s) affected.')

    # cursor.execute('SELECT id, price, brand from product')
    # print(cursor.description)
    # for item in  cursor.description:
    #     print(item._name)

    conn.commit()

    conn.close()
    # quit()