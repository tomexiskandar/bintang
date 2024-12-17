import os
import sys
from pathlib import Path
import json
import pyodbc

    
import bintang

if __name__ == '__main__':

    bt = bintang.Bintang('my bintang')
    bt.create_table('Person X')
    person = bt.get_table('Person X')

    person.insert([1,'John','Smith','1 Station St'], ['id','name','sur name','address'])
    person.insert([2,'Jane','Brown','Digging','8 Parade Rd'], ['id','name','sur name','address'])
    person.insert([3,'Okie','Dokey','7 Ocean Rd'], ['id','name','sur name','address'])
    person.insert([4,'Maria','Digging'], ['id','name','hobby'])
    # person.insert([5,'Bing','Digging'], ['id','name','hobby'])

    # connect to postgesql
    conn = pyodbc.connect('DRIVER={PostgreSQL ODBC Driver(UNICODE)};SERVER=localhost;PORT=5432;DATABASE=testdb;UID=userdb;PWD=password')
    cursor = conn.cursor()
    # cursor.execute('''
    #                CREATE TABLE orders (
    #                    id serial NOT NULL PRIMARY KEY,
    #                    info json
    #                    );
    #                ''')
    # conn.commit() 
    print(conn)

    createsql = bt['Person X'].gen_create_sqltable('postgresql')
    print(createsql)
    cursor.execute('DROP TABLE IF EXISTS "Person X"')
    cursor.execute(createsql)
    conn.commit()
    
    
    columns = ['id','name','sur name','address','hobby']
    ret = person.to_sql(conn, 'Person X', columns, schema='public', method='string')
    conn.commit()
    print(f'{ret} record(s) affected.')
    quit()


    # insert_sql = ''
    # for idx, row in ft['FishingClub'].iterrows():
    #     insert_sql = "INSERT INTO orders (info) VALUES('{}')".format(json.dumps(row))
    #     print(insert_sql)
    #     cursor.execute(str(insert_sql))
    #     cursor.commit()
        #print(idx, row)

    # read_psql
    cursor.execute('SELECT id, info FROM orders;')    
    row = cursor.fetchone()
    while row:
        print(row.info)
        row = cursor.fetchone()


    # cursor.execute('SELECT id, info FROM orders;')    
    # rows = cursor.fetchmany(10)
    # while rows:
    #     print(rows)
    #     rows = cursor.fetchmany(10)

    cursor.close()
    conn.close()
        






