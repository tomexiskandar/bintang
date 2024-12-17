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
    filepath = r"C:\Users\60145210\Documents\AFMO Data\ems_repo\ASNSW Ambulance.xlsx"
    print(os.path.basename(filepath))
    filename = os.path.basename(filepath)
    tablename = filename[:10]

    bt.read_excel(filepath,"Sheet1")
    #bt["Sheet1"].set_data_props()
  
    
    #print(bt['Sheet1']) # to get how many columns have data
    print(len(bt['Sheet1']))
 


    for idx, row in bt.iterrows("Sheet1"):
        bt["Sheet1"][idx,"Filename"] = tablename
        #bt["Sheet1"][idx,"BME Number"] = 'TEMP ' + str(idx)

    
    # for idx, row in bt.iterrows("Sheet1"):
    #     print(idx, row)  
    #     break

 

    
    # create a CopyConf
    cc = copyconf.CopyConf()
    cc.description = "copy excel to mssql"

    # create a copy object
    c = copyconf.Copy("assets")
    c.source_type = "bin_table"
    c.target_type = "sql_table"

    # create a source object 
    src_obj = copyconf.BinTableConf()
    src_obj.bin_table = bt["Sheet1"]
    c.source = src_obj # assign to a copy

    # create a target object
    trg_obj = copyconf.SQLTableConf()
    trg_obj.conn_str =  "DRIVER={ODBC Driver 17 for SQL Server};SERVER=EHL5CD8434KLM;PORT=1443;DATABASE=biomed;Trusted_Connection=yes;"
    trg_obj.schema_name = "dbo"
    trg_obj.table_name = "assets"
    c.target = trg_obj
    c.optional['insert_method'] = 'batch'

    ### drop table for testing ???????????????????
    ####???????????????????????????????????????????????
    # res = copython.drop_table(trg_obj.conn_str,'dbo','assets_test2')
    

    # define column mapping.
    # in this simple example we need to create colmap objects and add them to the copy
    # of course there is better way to do this eg. get table's columns from database
    # and create a for-loop process to add any colmap.
    colmap = copyconf.ColMapConf("Asset ID","BmeNumber")
    c.colmap_list.append(colmap)
    colmap = copyconf.ColMapConf("Serial Number","SerialNumber")
    c.colmap_list.append(colmap)
    colmap = copyconf.ColMapConf("Make","ManufacturerName")
    c.colmap_list.append(colmap)
    colmap = copyconf.ColMapConf("Model","ModelNumber")
    c.colmap_list.append(colmap)
    colmap = copyconf.ColMapConf("Station","SiteName")
    c.colmap_list.append(colmap)
    colmap = copyconf.ColMapConf("Description","Name")
    c.colmap_list.append(colmap)
    colmap = copyconf.ColMapConf("Filename","Filename")
    c.colmap_list.append(colmap)

    # add this c (Copy instance) into cc (CopyConf)
    cc.add_copy(c)

    # call copy_data
    res = copython.copy_data(cc, debug=True)
    print("res={}".format(res))


    # for idx, row in src_obj.flytab.iterrows():
    #     print(idx, row)




    
       
    
