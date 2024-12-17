import sqlite3
from unicodedata import name

if __name__ =='__main__':

    conn = sqlite3.connect('myrb.db')
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
    for row in cur.fetchall():
        # print(row.keys())
        print('deleting {} table'.format(row['name']))
        # delete table 
        try:
            cur.execute("DROP TABLE '{}';".format(row['name']))
            cur.execute("SELECT 1 (SELECT from sqlite_sequence where name=?", tuple(row[name]))
            ret = cur.fetchone([0])
            print(ret)
            if ret:
                cur.execute("delete from sqlite_sequence where name=?", tuple(row[name]))
        except:
            pass
    cur.close()
    conn.close()
         