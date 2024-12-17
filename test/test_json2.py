# def get_json_fromfile(self,path):
#         #print('parsing json file "{}"'.format(path))
#         jsondata = json.load(open(path))
#         self.jsondata_cfg = jsondata

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


if __name__ == '__main__':
    # get json data
    jsondata = json.load(open("account.json"))

    # create a flytab
    ft = bintang.Bintang("myft")
    #tablepaths = ['/balances/special']
    ft.read_dict(jsondata)
    
    print("\n------------ in main() ------------------")
    # rename table name
    # tab = ft.tables['/balances/special']
    # ft.rename_table('/balances/special','specialx')
    # tab.name = 'specialx'
    #ft.rename_table('/balances', 'Balances')
    # for k, v in ft.__tables.items():
    #     print('table:', k, 'name:',v.name, 'columns:', v.get_columnnames())
    print(ft.get_tablenames())

    ft.describe('/')
   
    
    
    # tab = ft.get_table('/menu/items')
  



    # print("\n-----------result_as dict---------------")
    # for idx, row in ft.iter_rows('/menu/items'):
    #     print(idx, row)

    #tab.print_asdict()    
    print('hello')
    #ft['/menu/items'].print_aslist()
    # print(ft['/menu/items'])
    # #print(ft[0])
    # table = ft['/menu/items']
    # ft['/menu/items'].print_aslist()

    # for idx, row in ft['/menu/items'].iter_rows():
    #     print(idx, row['label'])

    
    