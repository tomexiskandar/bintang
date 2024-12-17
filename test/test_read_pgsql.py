import bintang
import pyodbc

if __name__ == '__main__':

    # connect to pgsql
    conn_str = 'DRIVER={PostgreSQL ODBC Driver(UNICODE)};SERVER=localhost;PORT=5432;DATABASE=testdb;UID=userdb;PWD=password'
    conn = pyodbc.connect(conn_str)
    sql_str = 'SELECT * FROM "Person X" WHERE "sur name"=?'
    params = ('Dokey')

    bt = bintang.Bintang()
    bt.create_table('Person')
    bt['Person'].read_sql(conn, sql_str, params)

    for idx, row in bt['Person'].iterrows():
        print(idx, row)

    bt['Person'].print()

    conn.close()    


    