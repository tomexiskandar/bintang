
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
    
    dict_obj = {}
    dict_obj['person'] = []
    
    
    
    person = bt.get_table("Person")
    person.insert([1,"John","Smith","1 Station Street"], ["id","name","surname","address"] )
    person.insert([2,"Jane",'Brown',"Digging","8 Parade Road"], ["id","name","surname","hobby","address"])
    # person.insert([3,"Nutmeg","Spaniel",'7 Ocean Road'], ["id","name","surname","Address"])
    # person.insert([4,"Maria","Digging",'7 Heaven Avenue'], ["id","name","hobby","Address"])
    # person.insert([5,"Bing","Digging",'3 Tree Crescent'], ["id","name","hobby","Address"])

    
    for idx, row in bt['Person'].iterrows(['id','name','surname','address']):
        print(idx,row)
        address = row['address'].split()
        print(address)
        address_obj = {'number':address[0], 'street':address[1], 'street_type':address[2]}
        person_obj = {'id':row['id'], 'name':row['name'], 'surname':row['surname'], 'address':address_obj}
        dict_obj['person'].append(person_obj)
    
    print('-----')
    print(dict_obj)
   
    print('-----')
    
    dict_obj['PersonDetails'] = []
    bt.create_table("PersonDetails")
    bt['PersonDetails'].insert(['1','Blogging',True], ['person_id','hobby','is_meat_eater'])
    bt['PersonDetails'].insert(['2','Reading',None], ['person_id','hobby','is_meat_eater'])
    for idx, row in bt['PersonDetails'].iterrows():
        print(idx, row)
        dict_obj['PersonDetails'].append(row)

    print('---')
    print(dict_obj)

    dict_1 = {'person': [{'id': 1, 'name': 'John', 'surname': 'Smith', 'address': {'number': '1', 'street': 'Station', 'street_type': 'Street'}}, {'id': 2, 'name': 'Jane', 'surname': 'Brown', 'address': {'number': '8', 'street': 'Parade', 'street_type': 'Road'}}], 'PersonDetails': [{'person_id': '1', 'hobby': 'Blogging', 'is_meat_eater': True}, {'person_id': '2', 'hobby': 'Reading', 'is_meat_eater': None}]}

    with open('dict_person.json', 'w') as fp:
        json.dump(dict_1,fp)
   
    