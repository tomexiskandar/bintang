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

    
from bintang.core import Bintang


if __name__ == '__main__':
    # get json data
    jsondata = json.load(open("menu.json"))
    print(type(jsondata))
   
    # create a bintang
    ft = Bintang("mybin")

    ft.read_dict(jsondata)
    
    print("\n------------ in main() ------------------")

    print(ft.get_tables())

    
    
    
    tab = ft.get_table('/menu/items')
  



    # print("\n-----------result_as dict---------------")
    # for idx, row in ft.iter_rows('/menu/items'):
    #     print(idx, row)

    #tab.print_asdict()    
    print('hello')
    ft['/menu/items'].set_data_props()
    # print(ft['/menu/items'])
    # quit()
    #print(ft[0])
    table = ft['/menu/items']
    ft['/menu/items'].print()

    for idx, row in ft['/menu/items'].iterrows():
        print(idx, row['label'])

    
    