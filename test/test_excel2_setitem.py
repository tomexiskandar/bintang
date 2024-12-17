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
    sys.path.insert(0,os.path.join(PROJECT_ROOT,"flytab"))
    sys.path.insert(1,r"C:\Users\60145210\Documents\Projects\copython")
    #print(sys.path)    
from flytab.core import Flytab
import copython
from copython import copyconf


if __name__ == '__main__':

    
    ft = Flytab("myft")
    filepath = r"C:\Users\60145210\Documents\Projects\flytab\test\far west.xlsx"
    print(os.path.basename(filepath))
    
    ft.read_excel(filepath,"test")
    print(ft)
    #rename columnname
    ft["test"].rename_columnname('Asset No.','Asset No')
    print(ft['test'])
    print(ft['test'][1]["Model","Name"])
    #ft['test']['Filename'] = ft['test']['Name']

    for idx, row in ft["test"].iterrows():
        print(idx, row)
        if idx == 5: exit()
    
