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
    bt.create_sql_linked_table('sqllinkedtable', conn, sql_str=f'SELECT * FROM {tablename};')
    expected_columns = ['id', 'name', 'surname', 'address','hobby','score']
    assert Counter(expected_columns) == Counter(bt['sqllinkedtable'].get_columns())
    for idx, row in bt['sqllinkedtable'].iterrows(row_type='list'):
        assert 1 == idx
        assert isinstance(row, list)
        break
    for idx, row in bt['sqllinkedtable'].iterrows(row_type='dict'):
            assert 1 == idx
            assert isinstance(row, dict)
            break

    # to csv
    this_path = Path(__file__).parent
    fpath = this_path / "local_person.csv" 
    fpath.parent.mkdir(parents=True, exist_ok=True)
    bt['sqllinkedtable'].to_csv(fpath)

    # create a new table to read that csv
    bt.create_table('readcsv')
    bt['readcsv'].read_csv(fpath)
    assert Counter(expected_columns) == Counter(bt['readcsv'].get_columns())
    assert len(p) == len(bt['readcsv'])

    conn.close()
    
    