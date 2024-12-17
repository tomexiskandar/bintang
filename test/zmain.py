from flytab import FlyTab
from time import time
import sys

def f():
    print('ff')

if __name__ == "__main__":
    ft = FlyTab("my flytab")
    #ft.create_table("afmo table",["model","spec","manufacturer"])
    ft.create_table("afmo_table")



    print('ft.tables before adding data',ft.tables["afmo_table"].columns)


    starttime = time()

    tab = ft.tables["afmo_table"]
    for i in range(2):
        tab._insert(["model","spec","manufacturer"],["abc","light","Phillips"])
        # tab.insert(["model","spec","manufacturer"],["xyz","steriliser","GENERIC"])
        # tab.insert(["model"],["abc"])
        # tab.insert(["model"],["xyz"])
        #ft.tables["afmo_table"].insert(["model","spec"],["pqr","rack"])
        #ft.insert("afmo_table",["model","mycol"],["pqr","rack"])
        tab._insert(["model","test"],[("pqr","cabinet"),("ppp","drawer")])
    #ft.insert("afmo_table",["model","mycol"],["pqr","rack"])

    print(time()-starttime,"seconds")
 
    print("rows count:",len(ft.tables["afmo_table"].rows))

    print (list(ft.tables["afmo_table"].rows.keys())[0])


    print('ft.tables after adding data',ft.tables["afmo_table"].columns)

    # res = ft.select("afmo_table",["model"])

    print(sys.getsizeof(ft.tables["afmo_table"]))

    #tab.drop_column("mycol")
    ft.drop_column("afmo_table","mycol")

    print('ft.tables after dropping a column',ft.tables["afmo_table"].columns)
    print('getcolumnnames()',ft.tables["afmo_table"].get_columnnames())
    # for idx, row in tab.rows.items():
    #     print(repr(row))

    #ft.print("afmo_table",top=6)   

    print("----")
    columnnames = ["spec"]
    res_dict = {}
    res_rowid = 0
   
    #for rowid, row in tab.scan_rows(f()): 
    # for rowid, row in ft.tables['afmo_table'].scan_rowsx(f()):
    #     print('...>',rowid,row) #row.get_cells(tab.columns))
    #     res_row = {}
    #     for columnname in columnnames:
    #         if columnname in row:
    #             #print(f'yes columnname {columnname} exists.')
    #             print(row[columnname])
    #             res_rowid = rowid
    #             res_row[columnname] = row[columnname]
                
    #         else:
    #             pass
    #            # print(f'columnname {columnname} do not exist.')
    #     res_dict[rowid] = res_row
    # print('----')
    columns = ft.tables['afmo_table'].columns
    for idx, row in ft.tables['afmo_table'].scan_rows():
        print(idx, row.get_dict(columns))
        #print(idx, row)
     
        

    #ft.select()
    #tab.scan_rows()

    # columnnames = ["model","spec","test"]
    # with ft.select(columnnames,"afmo_table","new_table") as tab:
    #     #cont.drop_column("spec")
    #     #tab.print()
    #     print(tab.get_columnid("spec"))
    # ft.select(columnnames,"afmo_table","new_table")

    #ab = ft.tables["new_table"]

    # for rowid,row in list(ft.tables["afmo_table"].rows.items()):
    #     print(rowid,row)
    #     print(ft.tables["afmo_table"].get_columnid("model"))
    #     if row.cells[ft.tables["afmo_table"].get_columnid("model")].value == "pqr":
    #         #ft.tables["afmo_table"].delete(rowid)
    #         del ft.tables["afmo_table"].rows[rowid]
       

    # print("----")
    # del ft.tables["afmo_table"].rows[8]
    # for rowid,row in ft.tables["afmo_table"].rows.items():
    #     print(rowid,row)

    
    
    # for row in ft.tables["afmo_table"].rows.items():
    #     print(row)
    

    # for idx,row in ft.tables["afmo_table"].rows.items():
    #     print(idx,str(row))


    quit()