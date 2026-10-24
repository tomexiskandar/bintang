import pyodbc
from bintang import Bintang
from collections import Counter

if __name__ == '__main__':
    bt = Bintang('my bintang')
    tablename = 'person_test'
    bt.create_table(tablename)
    p = bt.get_table(tablename)

    p.insert(['id','name','surname','address'], [1,'John','Smith','1 Station St'])
    p.insert(['id','name','surname','hobby','address'], [2,'Jane','Brown','Digging','8 Parade Rd'])
    p.insert(['id','name','surname','Address'], [3,'Okie','Dokie','7 Ocean Rd'])
    p.insert(('id','name','hobby','Address'), (4,'Maria','Digging','7 Heaven Ave'))
    p.insert(['id','name','score','address'], [3,'Nutmeg',67,"7 O'cean Rd"])

    conn = pyodbc.connect("DRIVER={ODBC Driver 17 for SQL Server};SERVER=localhost;PORT=1443;DATABASE=test;Trusted_Connection=yes;",autocommit=True)  
    cursor = conn.cursor()
    cursor.execute(f'DROP TABLE IF EXISTS {tablename}')
    createsql = p.gen_sql_stmt_create_table_dev('sqlserver')
    cursor.execute(createsql)
    conn.commit()

    # send data to sql
    columns = ['id','name','surname']
    ret = p.to_sql(conn, tablename, method='prepared', max_rows=3)
    assert 5 == ret # 5 records affected
    conn.commit()

    # read sql
    bt.create_table('readsql')
    bt['readsql'].read_sql(conn, sql_str=f'SELECT * FROM {tablename};')
    expected_columns = ['id', 'name', 'surname', 'address','hobby','score']
    assert Counter(expected_columns) == Counter(bt['readsql'].get_columns())
    assert 67 == bt['readsql'].get_value('score', where= lambda row: row['name'].lower() == 'nutmeg')

    conn.close()
    
    