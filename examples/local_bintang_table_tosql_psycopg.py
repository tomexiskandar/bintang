from bintang import Bintang
from collections import Counter
from pathlib import Path
from dotenv import load_dotenv

# Find .env from the project root
project_root = Path(__file__).resolve().parents[1]
load_dotenv(project_root / ".env")

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

    import psycopg
    # read database connection details from environment variables
    import os
    POSTGRES_HOST = os.getenv('POSTGRES_HOST')
    POSTGRES_DATABASE = os.getenv('POSTGRES_DATABASE')
    POSTGRES_USERNAME = os.getenv('POSTGRES_USERNAME')
    POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD')
    conn_str_psycopg = f"host={POSTGRES_HOST} port=5432 dbname={POSTGRES_DATABASE} user={POSTGRES_USERNAME} password={POSTGRES_PASSWORD}"
    conn = psycopg.connect(conn_str_psycopg)
    cursor = conn.cursor()
    cursor.execute(f'DROP TABLE IF EXISTS {tablename}')
    createsql = p.gen_sql_stmt_create_table_dev('postgresql')
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
    conn = psycopg.connect(conn_str_psycopg)
    cursor = conn.cursor()
    cursor.execute(f'SELECT COUNT(*) AS total_rows FROM {tablename}')
    total_rows = cursor.fetchone()[0]
    print(f'total_rows: {total_rows}')
    assert total_rows == ret * 2
    conn.close()
    