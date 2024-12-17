import datetime as dt
import time # to sleep
from scheduler import Scheduler
from scheduler.trigger import Monday, Tuesday
import functools
from bintang import Bintang
import pyodbc
# def foo(arg):
#     print(f'>>>>>>>>>>>>>>>>> foo() on cyclic schedule {arg} >>>>>>>>>')

# def bar(arg):
#     print(f'>>>>>>>>>>>>>>>>> bar() on cyclic schedule {arg} >>>>>>>>>')
schedule = Scheduler()
# #functools.partial(schedule.minutely,dt.time(second=15), foo, 'test')
# schedule.cyclic(dt.timedelta(seconds=10), bar, args=('blessed',))
CONN_STR = "DRIVER={ODBC Driver 17 for SQL Server};SERVER=localhost;PORT=1443;DATABASE=bme;Trusted_Connection=yes;"
def upload_excel(path, sheetname):
    bt = Bintang()
    tab = bt.create_table('vbis')
    tab.read_excel(path, sheetname)
    #tab.print()
    tab.drop_column('CreatedOn')
    # send to sql
    conn = pyodbc.connect(CONN_STR) # connect to db
    tab.to_sql(conn, 'vbis')
    conn.commit()
    conn.close()


path = r"C:\Users\60145210\Documents\SSIS\VBIS_20240107-SameColumns.xlsx"
#upload_excel(path,'Sheet1')

    
schedule.cyclic(dt.timedelta(minutes=1), upload_excel, args=(path,'Sheet1',))

while True:
    print(schedule)
    schedule.exec_jobs()
    time.sleep(5)