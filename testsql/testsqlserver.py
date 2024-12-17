
import pyodbc # for connecting sqlserver
import sqlite3 # for connecting sqlite3

if __name__ == '__main__':
    print('in main')
    conn3 = sqlite3.connect('biomed.db')
    #conn3.row_factory = sqlite3.Row # to get a resultset as dict
    cur3 = conn3.cursor()

    # create a table
    sql_get_table = '''SELECT name from sqlite_master 
                       WHERE type='table' and name=?'''
    res = cur3.execute(sql_get_table,('specs',)).fetchone()
    if res:
        print('table exists')
    else:
        print('table does not exist')
        conn3.execute('''CREATE TABLE specs
                (RowNumber int, Name text, ManufacturerName text, ModelNumber text, ModelName text,
                    EntryID int, Cat int, IsMatched text, CatDesc Text, Record int)''')
       
   
    # prepare sql to insert data into sqlite3
    sql_insert3 = '''INSERT INTO specs (RowNumber, Name, ManufacturerName, ModelNumber, ModelName, EntryID, Cat, IsMatched, CatDesc, Record)
                     VALUES (?,?,?,?,?,?,?,?,?,?)'''
    # get the specs data froms sql server
    conn = pyodbc.connect("DRIVER={ODBC Driver 17 for SQL Server};SERVER=EHL5CD8434KLM;PORT=1443;DATABASE=biomed;Trusted_Connection=yes;")
    cur = conn.cursor()
    cur.execute("SELECT RowNumber, Name, ManufacturerName, ModelNumber, ModelName, EntryID, Cat, IsMatched, CatDesc, Record FROM dbo.view_uncleansed_specs")
    columnnames = [x[0] for x in cur.description]
    print(columnnames)
    for idx, row in enumerate(cur.fetchall(),1):
        row_dict = dict(zip(columnnames,row))
        conn3.execute(sql_insert3,row)
        conn3.commit()
        print(row_dict)
        if idx==1000: break

    # validate table specs in sqlite3
    cur3.execute('SELECT * FROM specs;')
    for row in cur3.fetchall():
        print(row)


    cur3.close()
    conn3.close()
    cur.close()
    conn.close()    


    


    
