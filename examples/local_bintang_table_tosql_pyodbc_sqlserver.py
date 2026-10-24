import pyodbc
from bintang import Bintang
from collections import Counter
import os
from dotenv import load_dotenv
from pathlib import Path

project_root = Path(__file__).resolve().parents[1]
load_dotenv(project_root / ".env")

driver = os.getenv("SQLSERVER_DRIVER")
server = os.getenv("SQLSERVER_HOST")
database = os.getenv("SQLSERVER_DATABASE")
trusted = os.getenv(
    "SQLSERVER_TRUSTED_CONNECTION", "yes"
).strip().lower() in ("yes", "true", "1")

conn_str = (
    f"DRIVER={{{driver}}};"
    f"SERVER={server};"
    f"DATABASE={database};"
    f"Encrypt={os.getenv('SQLSERVER_ENCRYPT', 'yes')};"
    f"TrustServerCertificate={os.getenv('SQLSERVER_TRUST_SERVER_CERTIFICATE', 'no')};"
)

if trusted:
    conn_str += "Trusted_Connection=yes;"
else:
    username = os.getenv("SQLSERVER_USERNAME")
    password = os.getenv("SQLSERVER_PASSWORD")
    print(f'username: {username}, password: {password}')

    if not username or not password:
        raise ValueError("SQL Server username and password are required")
    conn_str += "Trusted_Connection=no;"


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

    conn = (
        pyodbc.connect(conn_str)
        if trusted
        else pyodbc.connect(conn_str, UID=username, PWD=password)
    )
    cursor = conn.cursor()
    cursor.execute(f'DROP TABLE IF EXISTS {tablename}')
    createsql = p.gen_sql_stmt_create_table_dev('sqlserver')
    cursor.execute(createsql)
    conn.commit()

    # send data to sql
    columns = ['id','name','surname']
    ret = p.to_sql(conn, tablename, method='prepared', max_rows=3)
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
    