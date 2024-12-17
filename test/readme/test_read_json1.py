
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
    json_str = '{"Page:": 100, "Time": "2033-09-05T00:00:00Z", \
               "Person": [{"id": 1, "name": "John", "surname": "Smith", \
                            "Address": {"number": 1, "street": "Station", "street_type": "Street"}}, \
                          {"id": 2, "name": "Jane", "surname": "Brown", \
                            "Address": {"number": 8, "street": "Parade", "street_type": "Road"}}], \
                "PersonDetails": [{"person_id": "1", "hobby": "Blogging", "is_meat_eater": true}, \
                                 {"person_id": "2", "hobby": "Reading", "is_meat_eater": null,"LuckyNo": [13, 17, 19]}]}'
    # print(json.loads(json_str))
    
    bt = bintang.Bintang('From JSON')
    bt.read_json(json_str)

    print(bt)

    for idx, row in bt['/'].iterrows():
        print(idx, row)

    bt['/Person'].print()
    for idx, row in bt['/Person'].iterrows():
        print(idx, row)

    bt['/Person/Address'].print()
    for idx, row in bt['/Person/Address'].iterrows():
        print(idx, row) 

    bt['/PersonDetails'].print()
    for idx, row in bt['/PersonDetails'].iterrows():
        print(idx, row)

    bt['/PersonDetails/LuckyNo'].print()
    for idx, row in bt['/PersonDetails/LuckyNo'].iterrows():
        print(idx, row)    


    

    

   
    