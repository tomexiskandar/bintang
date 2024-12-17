
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

def column_tolist(table):
    res = []
    for idx, row in table.iterrows():
        res.append(row['name'])
    return res
    
        

if __name__ == "__main__":
    dict_obj = {
        'Page:': 100,
        'Time': '2033-09-05T00:00:00Z',
        'Person': [
            {'id': 1,'name': 'John','surname': 'Smith',
                'Address': {'number': 1, 'street': 'Station','street_type': 'Street'}
            },
            {'id': 2,'name': 'Jane','surname': 'Brown',
                'Address': {'number': 8,'street': 'Parade','street_type': 'Road'}
            }
        ],
        'PersonDetails': [
            {'person_id': '1', 'hobby': 'Blogging','is_meat_eater': True
            },
            {'person_id': '2','hobby': 'Reading','is_meat_eater': None,
                'LuckyDays': [13,17,19]
            }
        ]
    }

    # print(json.dumps(dict_obj))
    # json_str = '{"Person": [{"id": 1, "name": "John", "surname": "Smith", "Address": {"number": 1, "street": "Station", "street_type": "Street"}}, {"id": 2, "name": "Jane", "surname": "Brown", "Address": {"number": 8, "street": "Parade", "street_type": "Road"}}], "PersonDetails": [{"person_id": "1", "hobby": "Blogging", "is_meat_eater": true}, {"person_id": "2", "hobby": "Reading", "is_meat_eater": null, "LuckyDays": [13, 17, 19]}]}'
    # print(json.loads(json_str))

    # access address person 1
    res = dict_obj['Person'][1]['Address']['number']
    print(res)
        
    bt = bintang.Bintang('some tables')
    bt.read_dict(dict_obj)
    print(bt)
    #res = bt['/Person/Address'].get_value('number', where=lambda row: row[''])


    for idx, row in bt['/Person'].iterrows():
        print(idx, row)

    res = column_tolist(bt['/Person'])
    print(res)
    
    
    quit()
        


    for idx, row in bt['/Person/Address'].iterrows():
        print(idx, row) 

    res = bt['/Person/Address'].get_value('number', where=lambda row: row['Person']==1)
    print(res)

    




    # for idx, row in bt['/PersonDetails'].iterrows():
    #     print(idx, row)

    # for idx, row in bt['/PersonDetails/LuckyDays'].iterrows():
    #     print(idx, row)    


    

    

   
    