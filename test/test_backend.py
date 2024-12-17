import os
import sys
from pathlib import Path
import json
import pyodbc
import sqlite3

import bintang

if __name__ == "__main__":
    import time
    
    bt = bintang.Bintang("testy")
    
    path = Path(__file__).parent / "Person.db"
    if path.exists():
        path.unlink()

    conn=sqlite3.connect(path)
    person = bt.create_table("Person", conn=conn)
    cursor = conn.cursor()
    start = time.time()
    for i in range(20000):

        #person.insert({'id': 5, 'name': 'Bing', 'age': None, 'address': None, 'hobby': 'Digging'})
        person.insert((1,"John",[35,36],"1 Station St"),("id","name","age","address"))
        person.insert([2,"Jane",17,"Digging","8 Parade Rd"],("id","name","age","hobby","address"))
        person.insert((3,"Nutmeg","Fishing",'7 Ocean Rd'), ("id","name","hobby","Address"))
        person.insert((4,"Maria","Digging",None), ("id","name","hobby","Address"))
        person.insert((5,"Bing","Digging",None), ("id","name","hobby","Address"))
    # #res = person.get_columnid_sql('hobby')
    #print(res)
    # test = person._get_last_assigned_ord_pos()
    # print('hello',test)
    # quit()
    # person.order_columns_sql(['hobby'])
    #sql_update = "UPDATE Person SET cells = (SELECT json_set(Person.cells, '$.2', 'tome')) where idx=3"
    # cursor = conn.cursor()
    # cursor.execute(sql_update)

        #person.update_row_sql(i, 'name', 'feliz')
        person.update_row_sql_bycolid(i, 2, 'feliz')
        # person.update_row(i, 'name', 'feliz')

        if i % 5000 == 0:
            #print(i)
            conn.commit()
    conn.commit()
           
    # res = person.get_columns_sql()

    # print(len(person))
    # quit()
    #person.print() #only for memory/non backend
    #print(person)
    print('It took', time.time()-start, 'seconds.')
    print('rows affected:', len(person)) 
    quit()
    # conn.commit()
    # for idx, row in person.iterrows(columns=['name','age']):
    #     print(idx, row)
    
    # res = person.get_row_sql(1, columns=['id','name','hobby'], row_type='list')
    # print(res)
    person.update_row_sql(2, 'age', 13)

    #person.print()
    conn.close()
