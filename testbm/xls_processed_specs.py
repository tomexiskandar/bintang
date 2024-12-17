import os
import sys
import json
import pyodbc
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
    filepath = r"C:\Users\60145210\Documents\AFMO Data\ems_repo\Specs\specs redo-cat4 processed.xlsx"
    print(os.path.basename(filepath))
    filename = os.path.basename(filepath)
    tablename = filename[:10]

    bt.read_excel(filepath,"Sheet1")
    #bt["Sheet1"].set_data_props()
  
    # connect to db
    conn_str =  "DRIVER={ODBC Driver 17 for SQL Server};SERVER=EHL5CD8434KLM;PORT=1443;DATABASE=biomed;Trusted_Connection=yes;"
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    sql_update_entryid = """
    UPDATE dbo.specs SET cat=?
                        ,EntryID=?
    WHERE RowNumber=?;
    """
    sql_update_cat4 = """
    UPDATE dbo.specs SET cat=4
    WHERE RowNumber=?;
    """
    #print(bt['Sheet1']) # to get how many columns have data
    print(len(bt['Sheet1']))
    matches = 0
    for idx, row in bt['Sheet1'].iterrows():
        if row['cat'] in [1,2,3,4]:# and row['entry_id'] > 0:
            if row['RowNumber']==2201:print(idx, row)
            matches += 1
            # update db for the row
            cursor.execute(sql_update_entryid,[row['cat'], row['entry_id'], row['RowNumber']])
            conn.commit()
        # if row['cat'] == 4:
        #     cursor.execute(sql_update_cat4,[row['RowNumber']])
        #     conn.commit()
            # quit()
    
    print(matches)
    quit()
    
    



    
       
    
