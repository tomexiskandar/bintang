
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
    bt['Person'].insert([10,'John','Smith','1 Station St'], ['Id','Σίσυφος','straße','address'])
    bt['Person'].insert([20,'Jane','Brown','Digging','8 Parade Rd'], ['ID','Σίσυφος','straße','hobby','address'])
    bt['Person'].insert([30,'Okie','Dokie','7 Ocean Rd'], ['id','Σίσυφος','strasse','Address'])
    bt['Person'].insert((40,'Σίσυφος',['Digging','Chewing'],None), ('id','Σίσυφος','hobby','Address'))
    bt['Person'].insert((50,'ΣΊΣΥΦΟΣ','Digging','7 Heaven Ave'), ('id','ΣΊΣΥΦΟΣ','hobby','Address'))

    # for idx, row in bt['Person'].iterrows():
    #     print(idx, row)
    
   
    # test_greek = 'Σίσυφος'.upper() == 'ΣΊΣΥΦΟΣ'.upper()
    # print('greek',test_greek)
    # test_german = 'straße'.upper() == 'strasse'.upper()
    # print('german',test_german)
    
    #use 'ΣΊΣΥΦΟΣ' wil throw error coz 'Σίσυφος' firstly recorded column
    grouped = bt['Person'].groupby(['Σίσυφος'], # deprecated save_as 'grouped', 
                         group_count=True,
                         group_concat='id')

    
    bt.add_table(grouped)
    bt['grouped'].print()
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
    