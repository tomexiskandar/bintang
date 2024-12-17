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
    jsondata = json.load(open("widget.json"))

    # create a bintang
    ft = Bintang("mybin")
    #tablepaths = ['/balances/special']
    ft.read_json(jsondata)
    
    print("\n------------ in main() ------------------")

    print(ft.get_tablenames())

    
   
    for idx, row in ft['/widget/window'].iterrows():
        print(idx, row)

    
    