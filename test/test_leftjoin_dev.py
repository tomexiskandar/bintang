
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
    sys.path.insert(0,os.path.join(PROJECT_ROOT,'bintang'))

import bintang

if __name__ == '__main__':
    bt = bintang.Bintang('my bintang')
    
    bt.create_table('Person')
    person = bt.get_table('Person')
    person.insert((1,'john','Smith','1 Station St'),('id','name','surname','address'))
    person.insert([2,'Jane','Brown','Digging','8 Parade Rd'],('id','name','surname','hobby','address'),)
    person.insert((3,'Nutmeg','Spaniel','7 Ocean Rd'),('id','name','surname','Address'))
    person.insert((4,'Maria','Digging',None),('id','name','hobby','Address'))
    person.insert((5,'Bing','Digging',None),('id','name','hobby','Address'))


    bt.create_table('FishingClub')
    bt['FishingClub'].insert(('Ajes','Freeman','Active'),('FirstName','LastName','Membership'))
    bt['FishingClub'].insert(('John','Smith','Active'),('FirstName','LastName','Membership'))
    bt['FishingClub'].insert(('John','Brown','Active'),('FirstName','LastName','Membership'))
    bt['FishingClub'].insert(('Nutmeg','Spaniel','Active'),('FirstName','LastName','Membership'))
    bt['FishingClub'].insert(('Zekey','Pokey','Active'),('FirstName','LastName','Membership'))

    for idx, row in bt['FishingClub'].iterrows():
        print(idx, row)

    # create an inner join for table Person and Fishing Club
    # by using name and surname

    joined = bt.leftjoin('Person' #, ('name','surname')
                 ,'FishingClub'    #,    ['FirstName','LastName']
                 ,[('name','FirstName'), ('surname','LastName')]
                 ,into = 'Fisherman'
                 ,out_lcolumns=['name','address']
                 ,out_rcolumns=['Membership']
                 )
    bt.add_table(joined)             
    print(bt.get_tables())    
 
    for idx, row in joined.iterrows():
        print(idx, row)

    # access join table from bt
    bt['Fisherman'].print()

    quit()
    