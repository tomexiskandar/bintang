
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

import bintang

if __name__ == "__main__":
    bt = bintang.Bintang("my bintang")
    
    bt.create_table("Person")
    

    bt.get_table("Person")
    bt['Person'].insert([1,10,'John','Smith','1 Station St'], ['id','age','name','surname','address'])
    bt['Person'].insert([2,20,'Jane','Brown','Digging','8 Parade Rd'], ['id','age','name','surname','hobby','address'])
    bt['Person'].insert([3,30,'Okie','Dokie','7 Ocean Rd'], ['id','age','name','surname','Address'])
    bt['Person'].insert([3,30,'Okie','Dokie','7 Ocean Rd'], ['id','age','name','surname','Address'])
    bt['Person'].insert((4,40,'Maria','Digging','7 Heaven Ave'), ('id','age','name','hobby','Address'))
    # for idx, row in bt['Person'].iterrows():
    #     print(idx, row)
    print(bt['Person'].bing.name)
    
    
    
    
   
    grouped = bt['Person'].groupby(['id'],
                         sums = ['id','age']
                         
                         )

    bt.add_table(grouped)
    bt['grouped'].print()
    #grouped.print()
    quit()

    for idx, row in bt['grouped'].iterrows():
        print(idx, row)
        # for item in row['idx']:
        #     print(item)
    quit()

    bt['Person'].blookup(bt['grouped'],
                         [('ΣΊΣΥΦΟΣ','ΣΊΣΥΦΟΣ')],
                         ['group_count'])
    
    bt['Person'].print()

    # filtered = bt['Person'].filter(lambda row: row['group_count']>1)
    # print(filtered.get_columns())

    # print('')
    # for idx, row in filtered.iterrows():
    #     print(idx, row)
    # filtered.print()    

   
    
    #bt['Person'].to_excel('Person.xlsx')
    quit()
    