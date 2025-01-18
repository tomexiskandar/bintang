import os
import sys
import json
from openpyxl import load_workbook
use_package = False
if use_package is False:
    from pathlib import Path
    # define project root
    PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
    # add code directory
    sys.path.insert(0,os.path.join(PROJECT_ROOT,"bintang"))
    #print(sys.path)    
    
#from bintang.core import Bintang
import bintang

if __name__ == '__main__':
    test_dirname = Path(__file__).resolve().parent
    bt = bintang.Bintang()
    wb = load_workbook("contacts.xlsx", read_only=True, data_only=True)
    bt.read_excel(wb,["Person"])
    print(bt)
    #ft.copy_db()
    # ft.create_table_fromexcel(tes t_dirname / "contacts.xlsx","Group")
    bt.read_excel(wb,["FishingClub"])
    


    for idx, row in bt['Person'].iterrows():
        print('Person',idx, row)  

    for idx, row in bt['FishingClub'].iterrows():
        print('FishingClub', idx, row)
      
   
       
    # lookup membership from table FishingClub
    # ftt['Contacts'].tlookup(ftt['FishingClub'],["Name","Surname"],["First Name","Surname"]\
    #     ,["Membership","age"],["MemberStatus","AgeAsofToday"])

    # print('\n','-'*10,'after lookup')
    # for idx, row in ft.iterrows("Contacts"):
    #     print(idx, row)

    