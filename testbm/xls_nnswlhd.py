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
    filepath = r"C:\Users\60145210\Documents\AFMO Data\ems_repo\NNSWLHD.xlsx"
    print(os.path.basename(filepath))
    filename = os.path.basename(filepath)
    tablename = filename[:10]

    bt.read_excel(filepath,"Sheet1")
    #bt['Sheet1'].set_data_props()
    
    print(bt)
    #rename columnname
    #bt["Original Asset List"].rename_columnname('Asset No.','Asset No')
  
    
    
    
    for idx, row in bt.iterrows("Sheet1"):
        bt["Sheet1"][idx,"Filename"] = tablename
     

    
    # for idx, row in bt.iterrows("Sheet1"):
    #     print(idx, row)  
    #     quit()

    # quit()

    
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
    trg_obj.table_name = "Assets"
    c.target = trg_obj

    # define column mapping.
    # in this simple example we need to create colmap objects and add them to the copy
    # of course there is better way to do this eg. get table's columns from database
    # and create a for-loop process to add any colmap.
    colmap = copyconf.ColMapConf("BMENO","BmeNumber")
    c.colmap_list.append(colmap)
    colmap = copyconf.ColMapConf("TITLE","Name")
    c.colmap_list.append(colmap)
    colmap = copyconf.ColMapConf("SOFTWARE","SoftwareVersion")
    c.colmap_list.append(colmap)
    colmap = copyconf.ColMapConf("Manufacturer","ManufacturerName")
    c.colmap_list.append(colmap)
    colmap = copyconf.ColMapConf("SUPPLIER","Supplier")
    c.colmap_list.append(colmap)
    colmap = copyconf.ColMapConf("SERIAL_NO","SerialNumber")
    c.colmap_list.append(colmap)
    colmap = copyconf.ColMapConf("MODEL","ModelNumber")
    c.colmap_list.append(colmap)
    colmap = copyconf.ColMapConf("BRAND_NAME","Brand")
    c.colmap_list.append(colmap)
    colmap = copyconf.ColMapConf("Hospital","Hospital")
    c.colmap_list.append(colmap)
    colmap = copyconf.ColMapConf("COST","Price")
    c.colmap_list.append(colmap)
    colmap = copyconf.ColMapConf("DELIVERY","DeliveryDate")
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




    
       
    
