import os
import sys
from pathlib import Path
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0,os.path.join(PROJECT_ROOT,"bintang"))


import bintang
bt = bintang.Bintang()
print(bt)

bt.create_table('Person')
print(bt)

bt['Person'].insert(("id","name","age"),(1,"John",35))
bt['Person'].insert(("id","name","age"),[2,"Jane",17])
bt['Person'].insert(("id","name","hobby"),(3,"Nutmeg","Fishing"))
bt['Person'].insert(("id","name","hobby"),(4,"Maria","Digging"))
bt['Person'].insert(("id","name","hobby"),(5,"Bing","Digging"))

for idx, row in bt['Person'].iterrows():
    print(idx, row)

print(bt['Person']._Bing__columns)
col = bt['Person']._Bing__columns[2]
print(col)
print(bt['Person'].get_columnnames())

# loop through the table Person's rows to view what object the rows are made of.
for idx, row in bt['Person']._Bing__rows.items():
    print(idx, row)
