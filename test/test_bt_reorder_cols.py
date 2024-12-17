
from time import time
import sys

import os
import sys
from pathlib import Path
import json


use_package = False
if use_package is False:
    # definec project root
    PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
    # add code directory
    sys.path.insert(0,os.path.join(PROJECT_ROOT,"bintang"))

# from bintang.core import Bintang
# from bintang.bing import Bing
# from bintang.row import Row
import bintang

if __name__ == "__main__":
    bt = bintang.Bintang("my bintang")
    
    bt.create_table("Person")
    print('hello in main')

    print(bt)

    
    person = bt.get_table("Person")
    person.insert((1,"John",35,"1 Station St"),("id","name","age","address"))
    person.insert([2,"Jane",17,"Digging","8 Parade Rd"],("id","name","age","hobby","address"))
    person.insert((3,"Nutmeg","Fishing",'7 Ocean Rd'), ("id","name","hobby","Address"))
    # person.insert((4,"Maria","Digging",None), ("id","name","hobby","Address"))
    # person.insert((5,"Bing","Digging",None), ("id","name","hobby","Address"))

    person.delete_row(0)
    # changing ordinal position of a column in a table isn't good idea.
    # we just have to create a new table and populate that table with a desired order.
    print('-----')
    for idx, row in bt['Person'].iterrows(['name','address','id','age']):
        print(idx,row)
   
    
    # also, when use to_excel(), user can pass columns argument
    # try: Path.unlink('result.xlsx')
    # except: pass
    #person.to_excel('result.xlsx',index=False, columns=['name','address'])
    #person.to_excel('result.xlsx', columns = ['name','address'])

    # add record
    person.insert((4,"Maria","Digging",None), ("id","name","hobby","Address"))
    person.insert((5,"Bing","Digging",None), ("id","name","hobby","Address"))

    print('-----')
    for idx, row in bt['Person'].iterrows(['name','address','id','age']):
        print(idx,row)

    person.reindex()

    print('-----')
    for idx, row in bt['Person'].iterrows(['name','address','id','age']):
        print(idx,row)    


   

