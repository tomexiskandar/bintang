import pyodbc

conn = pyodbc.connect(r'Driver={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=C:\Users\60145210\Documents\Database1.accdb;')
cursor = conn.cursor()
cursor.execute('select * from Test')
   
for row in cursor.fetchall():
    print (row)