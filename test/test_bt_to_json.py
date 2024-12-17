
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
    person.insert((4,"Maria","Digging",None), ("id","name","hobby","Address"))
    person.insert((5,"Bing","Digging",None), ("id","name","hobby","Address"))

    #person._to_json_file('json_output.json')
    dict_obj = person.to_dict()
    print(dict_obj)
    json_str = json.dumps(dict_obj)
    print(json_str)
    # with open('output_json.json', 'w') as f:
    #     f.write(json.dumps(adict))
    with open('myfile.json', 'w') as fp:
        json.dump(dict_obj, fp) 





    
   

