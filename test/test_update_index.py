
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

from bintang.core import Bintang
from bintang.table import Bing
from bintang.row import Row

if __name__ == "__main__":
    ft = Bintang("my flytab")
    
    ft.create_table("Person")
    print('hello in main')

    print(ft)

    
    tab_person = ft.get_table("Person")
    tab_person.insert(["id","name","age","address"],[1,"John",35,"1 Station St"])
    tab_person.insert(["id","name","age","address"],[2,"Jane",17,"8 Parade Rd"])
    tab_person.insert(["id","name","hobby","Address"],[(3,"Nutmeg","Fishing",'7 Ocean Rd'),(4,"Maria","Snoozing",None),(5,"Bing","Digging",None)])
    

    for idx, row in ft["Person"].iterrows():
        print(idx,row)

    ft["Person"].set_index('name')
    
    
     