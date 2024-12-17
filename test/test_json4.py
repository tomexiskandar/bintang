
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
    
    data_dict = json.load(open('account4.json'))
    # create a flytab
    bt = bintang.Bintang("mybt")
    tablepaths = ['/balances/special']
    bt.read_dict(data_dict, tablepaths)
    
    print(bt)
    print('*'*5)
    for idx, row in bt['/balances/special'].iterrows():
        print(idx, row)
    # print('----')
    # for idx, row in bt['/balances'].iterrows():
    #     print(idx, row)
    # print('----')
    # for idx, row in bt['/balances/special'].iterrows():
    #     print(idx, row)
    # print('----')    

    # joined = bt.innerjoin('/balances'
    #                       ,'/balances/special'
    #                       ,[('/balances','/balances')]
    #                       ,'joined')

    # for idx, row in bt['joined'].iterrows():
    #     print(idx, row)

    # print(bt['/balances'])                      
    # print('calling set_child_tables....')
    # bt.set_child_tables()

    # print(bt['/balances'].children)
    
    