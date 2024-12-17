
from time import time
import sys

import os
import sys
# if not sys.warnoptions:
#     import warnings
#     warnings.simplefilter("ignore")
from pathlib import Path
import json


use_package = False
if use_package is False:
    # definec project root
    PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
    # add code directory
    sys.path.insert(0,os.path.join(PROJECT_ROOT,"bintang"))

# from bintang.core import Bintang
# from bintang.bing import Bing
# from bintang.row import Row
import bintang
# from bintang.table import match #move to bintang.core
from bintang import get_similar_values


if __name__ == "__main__":
    bt = bintang.Bintang("my bintang")
    bt.create_table("Person")
    person = bt.get_table("Person")
    person.insert((1,"John",35,"1 Station St"),("id","name","age","address"))
    person.insert([2,100,17,"Digging","8 Parade Rd"],("id","name","age","hobby","address"))
    person.insert((3,"Nutmeg","Fishing",'7 Ocean Rd'), ("id","name","hobby","Address"))
    person.insert((4,"Maria","Digging",None), ("id","name","hobby","Address"))
    person.insert((5,"Bing","Digging",None), ("id","name","hobby","Address"))
    
    p = bt['Personx']
    bt.drop_table('Personx')
    print(bt)
    quit()

    person.update('job','fisherman', where= lambda row: row['name']=='John')
    person.update('key', lambda row: str(row['id']) + row['name'])
    person.delete(lambda row: match(row['name'],'john'))
    person.print()
    # r = person.get_row_asdict(2, rowid=True)
    # print(r)
    # test = match(1,1)
    # print(test)
    # trial to use difflib.SequenceMatcher
    #res = person.validate_column('Mame')
    test = bintang.get_similar_values('tomex',['tomax','iskandar'])
    print(test)
    quit()
    res = person.validate_columns(['mame','aged'])
    res = person.validate_column('mame')
    print(res)
    for idx, row in person.iterrows(columns=['age','address']):
        print(idx, row)
    quit()

    if person.index_exists(1):
        print('hello')
        print('yes idx 1 in person')
    x = person.get_row(27)
    #x = person[27]
    print(x)
    quit()
    # person.set_data_props()
    # print(person.get_column_object('name').data_props)
    quit()
    person.print_columns_info()
    quit()
    person.print()
    quit()
    # tab2 = bt['Person'].get_index_lambda(lambda row : row['age']==17)
    # print(tab2)

    person.update_row(6, 'name', 'Johny')
    for idx, row in bt['Person'].iterrows():
        print(idx,row)

    quit()    

    print('-----')
    tab3 = bt['Person'].filter(where = lambda row: row['age']>=17 and row['hobby']=='Digging')
    for idx, row in tab3.iterrows():
        print(idx, row)

       
    
    quit()



    tab3.update('new'
               ,lambda row: str(row['age'] or '') + ' ' + row['address']+ ' yess'
               ,where = lambda row: row['age']>=17
               )

    for idx, row in bt['Person'].iterrows():
        print(idx,row)
    print('-----')


    bt.create_table('FishingClub')
    bt['FishingClub'].insert({"FirstName":"Ajes","LastName":"Freeman","Membership":"Active"})

    for idx, row in bt['FishingClub'].iterrows():
        print(idx, row)

    # print(bt['FishingClub'])    

    quit()
    

    # for idx, row in tab3.iterrows():
    #     print(idx,row)

    print('-----')    
    tab3.update('test'
                ,123)

    bt['Person'].update('age', 10, where=lambda row: row['name']=='Maria')  

    bt['Person'].update('newcolumn','hello')

    for idx, row in tab3.iterrows():
        print(idx,row)            

    quit()

    # print(tab_person.get_columnnames())
    bt.copy_table("Person","Orang")
    print(bt)
    
    print('-' * 10)
    tab_person.print_aslist()
   
    # stmt = """if `name`.upper() == 'JANE':
    #               retval = True"""
    
    stmt = """if `name` == 'Bing' or `name` == 'Jane':
                  retval = True"""
    # for word in stmt.split():
    #     print(word,len(word))                  
    res = tab_person.get_index_exec(stmt)  
    print(res)

    print(len(tab_person))
    quit()  
    
      
    print('-' * 10)
    for idx in tab_person.get_indexes():
        row = tab_person.get_row_asdict(idx)
        test = False
        if row['age'] is not None:
            if row['age'] > 30:
                print('\t True',row)
                test = True
        if row['address'] is not None:
            if row['address'] in ['7 Ocean Rd']:
                print('\t True',row)
                test = True
                tab_person.delete_row(idx)
        # if test:
        #     tab_person.update_row(idx,"lookedfor",'Yes')


    tab_person.drop_column('address')
   
    

    print('-' * 10)
    tab_person.print_asdict()
    print('-' * 10)
    
  
    print('--- iter_rows(orang) ---')
    for idx, row in ft.iterrows('orang'):
        print(idx, row)