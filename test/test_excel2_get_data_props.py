import os
import sys
import json
# import copython
# from copython import copyconf

use_package = False
if use_package is False:
    from pathlib import Path
    # define project root
    PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
    # add code directory
    sys.path.insert(0,os.path.join(PROJECT_ROOT,"bintang"))
    sys.path.insert(1,r"C:\Users\60145210\Documents\Projects\copython")
    #print(sys.path)    
from bintang.core import Bintang
import copython
from copython import copyconf


if __name__ == '__main__':

    
    bt = Bintang("mybt")
    #filepath = r"C:\Users\60145210\Documents\Projects\flytab\test\far west.xlsx"
    filepath = r"C:\Users\60145210\Documents\AFMO Data\ems_repo\NNSWLHD.xlsx"
    print(os.path.basename(filepath))
    
    bt.read_excel(filepath,"Sheet1")
    print(bt)
    
    

    for idx, row in bt["Sheet1"].iterrows():
        print(idx, row)
        if idx == 5: break

    #print(ft["test"].get_types_used('Name')) 
    
    bt['Sheet1'].set_data_props()

    print(bt['Sheet1'])
    
    
