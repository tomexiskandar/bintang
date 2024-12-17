
from time import time
import sys

import os
import sys
from pathlib import Path
import json



import bintang

if __name__ == "__main__":
    bt = bintang.Bintang("my bintang")
    
    bt.create_table("type_info")
    bt['type_info'].read_excel(r"C:\Users\60145210\Documents\Projects\bintang\test\sql_typeinfo.xlsx",'Sheet1')
    for idx, row in bt['type_info'].iterrows(columns=['type_name','column_size','literal_prefix']):
        print(idx, row)
    tab = bt['type_info'].filter(columns=['type_name','column_size','literal_prefix','literal_suffix'])#,\
                                #  where=lambda row: row['type_name'] in ['nvarchar','int'])
    # tab = bt['type_info'].filter()
    tab.print()
    jsn = tab.to_json()
    with open('sql_typeinfo.json', 'w') as f:
        f.write(jsn)
    quit()
    