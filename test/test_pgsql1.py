import os
import sys
from pathlib import Path
import json
import pyodbc


import bintang

class Person():
    def __init__(self):
        self.x = 'x'
        self.b = 1
    # def __ref__(self):
    #     return type(self).__name__
    
    # def __str__(self):
    #     return type(self).__name__ + ' obj'
    

       
    
 ### NOT WORKING AT THE MOMENT BECAUSE DRIVER CANNOT BE INTALLED DUE TO MS DEFENDER   
import psycopg2
import psycopg
from psycopg.types import TypeInfo

if __name__ == "__main__":
    bt = bintang.Bintang("test_postgresql")
    p = Person()
    bt.create_table("Product")
    prod = bt['Product']
    prod.insert({'id':1, 'brand': 'Shimano', 'class':'rod', 'name':'Extraction','price':299})
    prod.insert({'id':2, 'brand': 'Shimano', 'class':'rod', 'name':'Zodias Travel','price':399})
    prod.insert({'id':3, 'brand': 'Ugly Stik', 'class':'rod', 'name':'Balance II','price':63.99})
    prod.insert({'id':4, 'brand': 'Shimano', 'class':'rod', 'name':'Zodias Travel','price':399})
    # prod.insert({'id':5, 'brand': 'Abu Garcia', 'class':'rod', 'name':'Veritas 4.0','price':123.35})
    # prod.insert({'id':6, 'brand': 'Abu Garcia', 'class':'rod', 'name':'Veritas 4.0','price':123.35})
    # prod.insert({'id':7, 'brand': 'Ugly Stik', 'class':'rod', 'name':'Gold II','price':87.99})
    # prod.insert({'id':8, 'brand': 'Shimano', 'class':'reel', 'sub class': 'spinning', 'name':'Sedona F1','price':99.00})
    # prod.insert({'id':9, 'brand': 'Shimano', 'class':'reel', 'sub class':'spinning', 'name':'FX Series 4000','price':54.99})
    # prod.insert({'id':10, 'brand': 'Daiwa', 'class':'reel', 'sub class':'spinning', 'name':'Exceler LT','price':80.00})
    # prod.insert({'id':11, 'brand': 'Daiwa', 'class':'reel', 'sub class':'spinning', 'name':'Crossfire','price':54.00})

    # import pyodbc
    # conn = pyodbc.connect('DRIVER={PostgreSQL Unicode};SERVER=localhost;PORT=5432;DATABASE=testdb;UID=userdb;PWD=password')
    # cursor = conn.cursor()

    prod.print()
  
    conn = psycopg.connect(dbname="testdb", user="userdb", password="password", host="localhost", port=5432)
    # print(conn.xid)
    # if conn.xid:
    #     print('True')
    

   
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

    ### insert data
    columns = prod.get_columns()
    ret = prod.to_sql(conn, 'public', 'product', columns, method='string' ) #inject
    # ret = prod.to_sql(conn, 'public', 'product', columns, method='string' )
    #ret = prod.to_sql(conn, 'public', 'product', columns, method='prep')
    print(f'{ret} record(s) affected.')

    # cursor.execute('SELECT id, price, brand from product')
    # print(cursor.description)
    # for item in  cursor.description:
    #     print(item._name)

    conn.commit()

    conn.close()
    # quit()