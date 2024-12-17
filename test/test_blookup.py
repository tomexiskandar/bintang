
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
    # print('hello in main')

    # test = bintang.match('1',1)
    # print(test)
    # quit()

    # print(bt)
    x = 'abc'
    y = 'zyz'
    a = 1
    b = 3
    tup = ([x,y], (a,b))
    print(tup)
    for item in tup:
        print(item)
        print(item[0])
    
    res = [x[0] for x in tup]
    print(res)
    
    # quit()    

    # singleton = 'hello',1
    # print(singleton)
    # quit() 

    
    bt.get_table("Person")
    bt['Person'].insert([1,'John','Smith','1 Station St'], ['id','name','surname','address'])
    bt['Person'].insert([2,'Jane','Brown','Digging','8 Parade Rd'], ['id','name','surname','hobby','address'])
    bt['Person'].insert([3,'Okie','Dokie','7 Ocean Rd'], ['id','name','surname','Address'])
    bt['Person'].insert((4,'Maria','Digging','7 Heaven Ave'), ('id','name','hobby','Address'))
    #bt['Person'].add_column('Membership')
    bt['Person'].print()

    print('-----')
    bt.create_table("FishingClub")
    bt['FishingClub'].insert(['Ajes','Freeman','Active'], ['FirstName','LastName','Membership'])
    bt['FishingClub'].insert(['John','Smith','Active'], ['FirstName','LastName','Membership'])
    bt['FishingClub'].insert(['Jane','Brown','Active'], ['FirstName','LastName','Membership'])
    bt['FishingClub'].insert(['Nutmeg','Spaniel','Active'], ['FirstName','LastName','Membership'])
    bt['FishingClub'].insert(['Zekey','Pokey','Active'], ['FirstName','LastName','Membership'])

    bt['FishingClub'].print()


    # blookup membership from table fishing club
    # by using name and surname
    bt['Person'].blookup('FishingClub', \
                        [('name','FirstName')] \
                        #[('id','age'),('Surname','LastName')]\
                        #,[('Membership','MemberStatus')])
                        #,['FirstName'])
                        #,[('membership','address')]) # warning! replacing existing address column
                        ,[('Membership','Member Status')])

    print('----')
    bt['Person'].print(show_data_type = True, square=False)
        
    #     if row['name'] is not None and row['FirstName'] is not None:
    #         if row['name'] != row['FirstName']:
    #             print('\topss.. different case!')


    # print('----')
    # for idx, row in bt['FishingClub'].iterrows():
    #     print(idx, row)


    # res = bt['FishingClub'].get_value(lambda row: row['FirstName']+row['LastName'], lambda row: row['LastName']=='Pokey' and row['age']==6)
    # print(res)

    # export to excel
    # bt['FishingClub'].to_excel('FishingClub.xlsx')
    # bt['Person'].to_excel('Person.xlsx')
    # quit()
    