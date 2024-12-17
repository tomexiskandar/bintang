import pyodbc

if __name__ == '__main__':
    #conn_str =  "DRIVER={ODBC Driver 17 for SQL Server};SERVER=EHL5CD8434KLM;PORT=1443;DATABASE=biomed;Trusted_Connection=yes;"
    conn_str = "DRIVER={PostgreSQL Unicode(x64)};SERVER=localhost;PORT=5432;DATABASE=biomed;UID=postgres;PWD=postgres"
    conn = pyodbc.connect(conn_str)
    print(conn.getinfo(pyodbc.SQL_DATABASE_NAME))
    print(conn.getinfo(pyodbc.SQL_DBMS_NAME))
    print(conn.getinfo(pyodbc.SQL_DBMS_VER))
    print(conn.getinfo(pyodbc.SQL_DRIVER_ODBC_VER))
    

    crsr = conn.cursor()
    # get type info
    # sql_type_info = crsr.getTypeInfo(sqlType = 'varchar')
    # print([column[0] for column in crsr.description])
    # print(len([column[0] for column in crsr.description]))
    # for row in sql_type_info:
    #     print(row[0], row[5])

    # get column
    sql_columns = crsr.columns(table='testy', schema='public')
    print([column[0] for column in crsr.description])
    for row in sql_columns:
        print(row)
    





















