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
import bintang

if __name__ == '__main__':
    test_dirname = Path(__file__).resolve().parent
    bt = bintang.Bintang("mybt")
    #bt = Bintang("mybt", backend="sqlite")
    
    
    #bt.copy_db()
   
    bt.read_excel(test_dirname / "contacts.xlsx")
    for tab in bt.get_tables():
        print(tab, len(tab))

    
    # for idx, row in bt['FishingClub'].iterrows():
    #     print(idx, row)

    quit()
    
    #bt.copy_db()
    # print(bt)
    # bt.describe('Contacts')
    # print('hello')
    # print('Contacts columnnames:',bt.get_columnnames('Contacts'))
    # about = json.dumps(bt.gen_me())
    #print(bt)

    # print(bt['Group'])

    # bt['Group'].describe()
    # quit()
    
    # res = bt['Contacts'].validate_columnnames(["name","Age"])
    # print(res)

    

    # for idx, row in bt.iterrows("Contacts"):
    #     #print('yes',idx, row)
    #     if row["Name"] == "Zeke":
    #         print("that's my boy :)")

    for idx, row in bt.iterrows("FishingClubx"):
        print('main',idx, row)  
      
   
       
    # lookup membership from table FishingClub
    # btt['Contacts'].tlookup(btt['FishingClub'],["Name","Surname"],["First Name","Surname"]\
    #     ,["Membership","age"],["MemberStatus","AgeAsobtoday"])

    # print('\n','-'*10,'abter lookup')
    # for idx, row in bt.iterrows("Contacts"):
    #     print(idx, row)

    

    tab_joined = bt.innerjoin("Person", 
                              "FishingClub", 
                              [("Name",'First name'),("Surname","Surname")], 
                              "joined",
                              output_lcolumnnames=["name","hobby"],
                              output_rcolumnnames=["membership"]
               #,rowid=True
               )
 
    
           


    print('\n','-'*10,"tab_joined")
    for idx, row in tab_joined.iterrows(result_as='dict'):
        print(idx, row)

    

    # save to excel
    tab_joined.to_excel('output.xlsx', index = 'rownum')
    quit()    



