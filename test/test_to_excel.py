
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
   
    person = bt.get_table("Person")
    person.insert([1,"John","Smith","1 Station St"], ["id","name","surname","address"] )
    person.insert([2,"Jane",'Brown',"Digging","8 Parade Rd"], ["id","name","surname","hobby","address"])
    person.insert([3,"Nutmeg","Spaniel",'7 Ocean Rd'], ["id","name","surname","Address"])
    person.insert([4,"Maria","Digging",None], ["id","name","hobby","Address"])
    person.insert([5,"Bing","Digging",None], ["id","name","hobby","Address"])

    
    for idx, row in bt['Person'].iterrows():
        print(idx,row)
    print('-----')

    bt["Person"].to_excel('output_excel.xlsx',['Name','address'], index='index')

    