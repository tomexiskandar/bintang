import os
import sys
import json

use_package = False
if use_package is False:
    from pathlib import Path
    # define project root
    PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
    # add code directory
    sys.path.insert(0,os.path.join(PROJECT_ROOT,"bintang"))
    #print(sys.path)    
    
#from bintang.core import Bintang
from bintang.core import Bintang

if __name__ == '__main__':
    bt = Bintang("mybt")
    
    sheets = {}
    # bt.read_excel("contacts.xlsx")
    bt.read_excel("contacts.xlsx", ['Personx','Group'])
    print(bt)
    for idx, row in bt['Person'].iterrows():
        print(idx, row)
    quit()
    