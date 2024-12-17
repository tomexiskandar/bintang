import os
import sys
from pathlib import Path
import json
import pyodbc


use_package = False
if use_package is False:
    # definec project root
    PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
    # add code directory
    sys.path.insert(0,os.path.join(PROJECT_ROOT,"bintang"))

    
import bintang

class Person():
    def __init__(self):
        self.x = 'x'
        self.b = 1
    # def __ref__(self):
    #     return type(self).__name__
    
    # def __str__(self):
    #     return type(self).__name__ + ' obj'
    

       
    
    

if __name__ == "__main__":
    bt = bintang.Bintang("my bintang")
    p = Person()
    bt.create_table("Product")
    prod = bt['Product']
    prod.insert({'id':1, 'brand': 'Shimano', 'name':'Hook','price':1.60})
    prod.insert({'id':2, 'brand': 'Ugly Stick', 'name':'Sinker','price':1.20})
    prod.insert({'id':3, 'brand': 'Shimano', 'name':'Reels','price':75})
    prod.insert({'id':4, 'brand': 'Abu Garcia', 'name':'Reels','price':75})
    prod.insert({'id':2, 'brand': 'Ugly Stick', 'name':'Sinker','price':1.20})

    
    # for k, v in prod._Table__columns.items():
    #     print(k, v.name, v.order)
    # prod.order_columns(['brand','price'])
    # # for idx, row in prod._Table__rows.items():
    # #     print(idx, row)
    # columns = prod.get_columns()
    # print(columns)
    # quit()

    prod.copy_index(column='index', at_start=True)

    print()
    #bt['Product'].print(show_data_type=False)
    
    # for idx, row in prod.iterrows():
    #     print(idx, row)
    
    bt['Product'].groupby(['brand','name'], 'grouped_product', 
                          group_count=True)

    # for idx, row in bt['grouped_product'].iterrows():
    #     print(idx, row)

    #print(bt['grouped_product'])
    print()
    #bt['grouped_product'].reindex()
    bt['grouped_product'].copy_index()
    bt['grouped_product'].print()
    for idx, row in bt['grouped_product'].iterrows(where=lambda row: row['brand']=='Shimano'):
        print(idx, row)

