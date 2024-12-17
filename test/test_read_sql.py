import bintang
import pyodbc

if __name__ == '__main__':

    # connect to sql server
    conn_str = "DRIVER={ODBC Driver 17 for SQL Server};SERVER=localhost;PORT=1443;DATABASE=test;Trusted_Connection=yes;"
    conn = pyodbc.connect(conn_str)
    sql_str = "SELECT * FROM Person WHERE LastName=?"
    params = ('Dokey')

    bt = bintang.Bintang()
    bt.create_table('Person')
    bt['Person'].read_sql(conn, sql_str, params)

    for idx, row in bt['Person'].iterrows():
        print(idx, row)

    bt['Person'].print()

    conn.close()    


    