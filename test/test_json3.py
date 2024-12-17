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
    test_dirname = Path(__file__).resolve().parent
    jsonfpname = test_dirname / "account3.json"
    
    jsondata = json.load(open(jsonfpname))
    
    
    # create a flytab
    ft = bintang.Bintang("myft")
    #tablepaths = ['/balances/special']
    ft.read_dict(jsondata) #, tablepaths= ['_balances'])
    
    print("\n------------ in main() ------------------")
    # rename table name
    
    # ft.rename_table('/balances/special','specialx')
    # tab.name = 'specialx'
    # ft.rename_table('/balances', 'Balances')
    # for k, v in ft.__tables.items():
    #     print('table:', k, 'name:',v.name, 'columns:', v.get_columnnames())
    print(ft.get_tables())

    table = 'root'
    if table in ft.get_tables():
        for idx, row in ft[table].iterrows():
            print('hello',idx, row)

    table = 'root_balances'
    if table in ft.get_tables():
        for idx, row in ft[table].iterrows():
            print('hello',idx, row)       
    
    
    quit()
    tab = ft.get_table('/balances')
    #print(tab['makerCommission'])
    for idx, row in tab.iterrows():
        print(idx, row)
        if row['asset'] == 'BNB':
            tab.update_row(idx,'special','dude')
            tab[idx,'specialty'] = 'bruh'
            # row['injected'] = 'yes'
            print('debug',tab[idx])
        else:
            tab[idx,'comment']= 'cool'
    
    
    print('--- post update---')
    for idx, row in tab.iterrows():
        print(idx,row)

    

    print('---original table---')    
    for idx, row in ft['/balances'].iterrows():
        print(idx, row)


    # print("\n-----------result_as dict---------------")
    # for idx, row in ft.iterrows('/balances):
    #     print(idx, row)

    #tab.print_asdict()    
    print('hello')
    #ft['/balances].print_aslist()
    # print(ft['/balances])
    # #print(ft[0])
    # table = ft['/balances]
    # ft['/balances].print_aslist()

    # for idx, row in ft['/balances/special'].iterrows():
    #     print(idx, row)
    row = ft['/balances'][1]
    print(row)
    asset = ft['/balances'][0]['specialty']
    print(asset)
    if '/' in ft.get_tables():
        tab1 = ft['/']
        for idx, row in tab1.iterrows():
            print('tab1',idx, row)
        print(ft['/'][0]['makerCommission'])



    
    