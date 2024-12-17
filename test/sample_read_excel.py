import os
import sys
from pathlib import Path
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0,os.path.join(PROJECT_ROOT,"bintang"))


import bintang

if __name__ == '__main__':
    bt = bintang.Bintang()
    bt.read_excel("contacts.xlsx","Person")
    for idx, row in bt['Person'].iterrows():
        print(idx, row)

