
import pyodbc # for connecting sqlserver
import csv

if __name__ == '__main__':
    # prepare sql to insert data into sqlite3
    sql_insert3 = '''INSERT INTO specs (RowNumber, Name, ManufacturerName, ModelNumber, ModelName, EntryID, Cat, IsMatched, CatDesc, Record)
                     VALUES (?,?,?,?,?,?,?,?,?,?)'''
    # get the specs data froms sql server
    conn = pyodbc.connect("DRIVER={ODBC Driver 17 for SQL Server};SERVER=EHL5CD8434KLM;PORT=1443;DATABASE=biomed;Trusted_Connection=yes;")
    cur = conn.cursor()
    cur.execute("SELECT RowNumber, Name, ManufacturerName, ModelNumber, ModelName, EntryID, Cat, IsMatched, CatDesc, Record FROM dbo.view_uncleansed_specs")
    columnnames = [x[0] for x in cur.description]
    print(columnnames)

    # iterate csv writer
    with open('specs_uncleased.csv', 'w', newline='' ) as f:
        writer = csv.writer(f, delimiter='|')
        # write the headers
        writer.writerow(['RowNumber', 'Name', 'ManufacturerName', 'ModelNumber', 'ModelName', 'EntryID', 'Cat', 'IsMatched', 'CatDesc', 'Record'])
        # write the row
        for idx, row in enumerate(cur.fetchall(),1):
            #row_dict = dict(zip(columnnames,row))
            writer.writerow(row)
            #print(row_dict)
            #if idx==10: break

    

    cur.close()
    conn.close()    


    


    
