import pyodbc
from bintang import Bintang
from collections import Counter
from pathlib import Path

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

    # USING PYODBC SQLITE DRIVER
    db_file = 'local_person_test.db'
    fpath =  Path(__file__).resolve().parent / db_file
    conn_str_sqlite3 = f"DRIVER={{SQLITE3 ODBC Driver}};Database={fpath};Direct=True;"
    conn = pyodbc.connect(conn_str_sqlite3)
    # create table
    cursor = conn.cursor()
    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {tablename} (
            name text,
            surname text,
            address text,
            hobby
            )
        """)
    cursor = conn.cursor()
    cursor.execute(f'DROP TABLE IF EXISTS {tablename}')
    createsql = p.gen_sql_stmt_create_table_dev('sqlite')
    cursor.execute(createsql)
    conn.commit()

    # send data to sql
    columns = ['id','name','surname']
    ret = p.to_sql(conn, tablename, method='prepared', max_rows=3)
    conn.commit()
    print('ret_prepared', ret)
    assert 5 == ret # 5 records affected
    ret = p.to_sql(conn, tablename, method='string', max_rows=3)
    print('ret_string', ret)
    assert 5 == ret # 5 records affected
    conn.commit()

    # check if the data has landed in the database
    cursor.execute(f'SELECT COUNT(*) AS total_rows FROM {tablename}')
    total_rows = cursor.fetchone()[0]
    print(f'total_rows: {total_rows}')
    assert total_rows == ret * 2
    conn.close()
    