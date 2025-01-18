
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
    bt['Person'].insert([1,'John','Smith','1 Station St'], ['id','name','surname','address'])
    bt['Person'].insert([2,'Jane','Brown','Digging','8 Parade Rd'], ['id','name','surname','hobby','address'])
    bt['Person'].insert([3,'Okie','Dokie','7 Ocean Rd'], ['id','name','surname','Address'])
    bt['Person'].insert((4,'Maria','Digging','7 Heaven Ave'), ('id','name','hobby','Address'))

    
    for idx, row in bt['Person'].iterrows():
        print(idx,row)
    print('-----')
    bt.create_table("FishingClub")
    bt['FishingClub'].insert([1, 'Ajes','Freeman','Active'], ['id', 'FirstName','LastName','Membership'])
    bt['FishingClub'].insert([2, 'jon','Smith','Active'], ['id', 'FirstName','LastName','Membership'])
    bt['FishingClub'].insert([3, 'Janey','Brown','Active'], ['id', 'FirstName','LastName','Membership'])
    bt['FishingClub'].insert([4, 'Jane','Brown','Active'], ['id', 'FirstName','LastName','Membership'])
    bt['FishingClub'].insert([5, 'Nutmeg','Spaniel','Active'], ['id', 'FirstName','LastName','Membership'])
    bt['FishingClub'].insert([6, 'Zekey','Pokey','Active'], ['id', 'FirstName','LastName','Membership'])

    for idx, row in bt['FishingClub'].iterrows():
        print(idx, row)

       


    # blookup membership from table fishing club
    # by using name and surname
    res = bt['Person'].flookup('FishingClub', \
                        #[('name','FirstName')], \
                        [('name','FirstName'),('Surname','LastName')],\
                        # ,[('Membership','MemberStatus'),('age','AgeAsofToday')])
                        #,['FirstName'])
                        #,[('membership','address')]) # warning! replacing existing address column
                        ['Membership', ('id', 'idoen')],
                        min_ratio=[70,80],
                        ret_matches=True)

    print('----')
    bt['Person'].print()

    # for idx, row in bt['Person'].iterrows(['name','Membership', 'FirstName']):
    #     print(idx, row)
    #     if row['name'] is not None and row['FirstName'] is not None:
    #         if row['name'] != row['FirstName']:
    #             print('\topss.. different case!')
    

    # for idx, row in res.iterrows():
    #     print(idx, row)

    res.print()

    quit()
    # print('----')
    # for idx, row in bt['FishingClub'].iterrows():
    #     print(idx, row)


    # res = bt['FishingClub'].get_value(lambda row: row['FirstName']+row['LastName'], lambda row: row['LastName']=='Pokey' and row['age']==6)
    # print(res)

    # export to excel
    bt['FishingClub'].to_excel('FishingClub.xlsx')
    bt['Person'].to_excel('Person.xlsx')
    quit()
    