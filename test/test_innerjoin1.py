
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

    
    person = bt.get_table("Person")
    person.insert([1,"John","Smith","1 Station St"], ["id","name","surname","address"] )
    person.insert([2,"Jane",'Brown',"Digging","8 Parade Rd"], ["id","name","surname","hobby","address"])
    person.insert([3,"Nutmeg","Spaniel",'7 Ocean Rd'], ["id","name","surname","Address"])
    person.insert([4,"Maria","Digging",None], ["id","name","hobby","Address"])
    person.insert([5,"Bing","Digging",None], ["id","name","hobby","Address"])

    
    for idx, row in bt['Person'].iterrows():
        print(idx,row)
    print('-----')

    bt2 = bintang.Bintang()
    bt2.create_table("FishingClub")
    bt2['FishingClub'].insert(['Ajes','Freeman','Active'], ['FirstName','LastName','Membership'])
    bt2['FishingClub'].insert(['John','Smith','Active'], ['FirstName','LastName','Membership'])
    bt2['FishingClub'].insert(['John','Brown','Active'], ['FirstName','LastName','Membership'])
    bt2['FishingClub'].insert(['Nutmeg','Spaniel','Active'], ['FirstName','LastName','Membership'])
    bt2['FishingClub'].insert(['Zekey','Pokey','Active'], ['FirstName','LastName','Membership'])

    for idx, row in bt2['FishingClub'].iterrows():
        print(idx, row)

    # create an inner join for table Person and Fishing Club
    # by using name and surname

    joined = bt.innerjoin('Person' #, ('name','surname')
                 ,bt2['FishingClub'] #'FishingClub'    #,    ['FirstName','LastName']
                 ,[('name','FirstName'), ('surname','LastName')]
                 ,'Fisherman'
                #  ,out_lcolumns=['name','address']
                #  ,out_rcolumns=['Membership']
                 )
    print(bt.get_tables())    

    for idx, row in bt['Fisherman'].iterrows():
        print(idx, row)

    
quit()
    