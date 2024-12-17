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

    
from bintang.core import Bintang as bg
import timeit

if __name__ == '__main__':
    # get json data
    jsondata = json.load(open("abc-local-stations.json"))

    # create a bintang
    ft = bg("mybinh")
    #tablepaths = ['/balances/special']
    st = timeit.default_timer()
    ft.read_json(jsondata)
    print(timeit.default_timer() - st)
    
    print(ft['/features']._Table__columns)
    
    print("\n------------ in main() ------------------")

    print(ft.get_tablenames())

   
    for idx, row in ft['/features'].iterrows():
        print(idx, row)
        break
        
    for idx, row in ft['/features/properties'].iterrows():
        print(idx, row)
        break

    tab_joined = ft.innerjoin('/features', '/features/properties', ['features'], ['features']\
               #,output_lcolumnnames=["name","surname","hobby"]
               #,output_rcolumnnames=["membership"]
               #,rowid=True
               )
    print('')
    for idx, row in tab_joined.iterrows():
        print(idx, row)
        # print(idx, row['name'], row['twitteraccount'], row['streetaddress'])
        break

    tab_joined.to_excel('output.xlsx', index = 'rowid')
    quit()    


    
    