import sqlite3
import json
import tempfile
from pathlib import Path
import os

class Besqlite():
    def __init__(self, name=None):
        self.name = name
        self.dbpath = None
        self.conn = None
        self.max_row_for_sql_insert = 100
        self.connect()
        self.create_bingtab()
    
    def connect(self):
        tempdirname = tempfile.gettempdir()
        dbfilename = self.name + '.db'
        # self.dbpath = Path(tempdirname, dbfilename)
        self.dbpath = Path(os.getcwd(), dbfilename)
        self.dbpath.unlink()
        self.conn = sqlite3.connect(self.dbpath)
        # print(os.getcwd())
        #sqlite3.connect(self.dbpath)
        print(self.dbpath)    
        

    def create_bingtab(self):
        """
            create a schema like table with prefix dunder to avoid name conflict
        """
        #conn = sqlite3.connect("{}.db".format(self.name))
        print(self.conn)
        cur = self.conn.cursor()
        
        cur.execute("SELECT EXISTS (SELECT 1 FROM sqlite_master WHERE type='table' AND name='__bintang__')")
        ret = cur.fetchone()[0]
        print(' ',ret)
        
        if  ret == 0:
            cur.execute("CREATE TABLE __bintang__ (id INTEGER PRIMARY KEY NOT NULL, name TEXT)")


    def add_table(self, tableid, tablename):
        cur = self.conn.cursor()
        # sql= "INSERT INTO __myrb (id, name) VALUES ({}, {});".format(self.get_tableid(tablename),tablename)
        # print(sql)
        cur.execute("INSERT OR IGNORE INTO __bintang__ (id, name) VALUES (:id, :name);",{"id":tableid, "name":tablename})
        self.conn.commit()
        cur.close()


    def create_table(self, name):
        cur = self.conn.cursor()
        cur.execute("SELECT EXISTS (SELECT 1 FROM sqlite_master WHERE type='table' AND name=:tablename)", {"tablename":name})
        ret = cur.fetchone()[0]
        if  ret == 0:
            cur.execute("CREATE TABLE '{}' (id INTEGER PRIMARY KEY NOT NULL, cells JSON)".format(name))
            cur.execute("CREATE TABLE '__columns_{}' (id INTEGER PRIMARY KEY NOT NULL, name TEXT)".format(name))
        cur.close()  


    def add_row(self, tablename, listof_jsoncells):
        cur = self.conn.cursor()
        params = []
        while len(params) < len(listof_jsoncells):
            params.append('(?)')
        sql = "INSERT INTO '{}' (cells) VALUES {}".format(tablename, ','.join(params))
        cur.execute(sql, listof_jsoncells)
        self.conn.commit()

    def add_column(self, tablename, columnid, columnname):
        cur = self.conn.cursor()
        sql = "INSERT INTO '__columns_{}' (id, name) VALUES (?,?)".format(tablename)
        cur.execute(sql, (columnid, columnname,))
        self.conn.commit()

    def _get_columnid(self,columnname):
        cur = self.conn.cursor()
        cur.execute("SELECT id FROM __columns_{} WHERE name=?;".format('__columns_' + self.name, columnname))
        ret = cur.fetchone()[0]
        if ret:
            return ret
        else:
            return

    def get_columnnames(self, tablename):
        # get columnnames from db and returen as dict of columnid : columnname
        columnnames = {}
        self.conn.row_factory = sqlite3.Row
        cur = self.conn.cursor()
        sql = "SELECT id, name FROM __columns_{}".format(tablename)
        for row in cur.execute(sql):
            columnnames[row['id']] = row['name']
        return columnnames

        
    def iterrows_asdict(self, tablename, columnnames):
        self.conn.row_factory = sqlite3.Row
        # get columnames
        db_columnnames = self.get_columnnames(tablename)
        # switch over value to key so it can be looked into for column name
        db_columnnames_keys_switched = {v: k for k, v in db_columnnames.items()}
        #print(db_columnnames_switched_keys)
        cur = self.conn.cursor()
        sql = "SELECT id, cells FROM {}".format(tablename)
        for row in cur.execute(sql):
            temp = json.loads(row["cells"])
            # set the keys (columnid) back to integer to follow original flytab's columnid type
            temp = {int(k):v for k,v in temp.items()} 
            #print("temp",temp)
            row_asdict = {}
            for columnname in columnnames:
                row_asdict[columnname] = temp[db_columnnames_keys_switched[columnname]]

            #print("mytest",row["id"], type(row["cells"]))
            yield row["id"], row_asdict #row["cells"]
        

    def get_row(self, idx, tablename):
        self.conn.row_factory = sqlite3.Row
        cur = self.conn.cursor()
        sql = "SELECT id, cells FROM {} WHERE id=?;".format(tablename)
        cur.execute(sql, (idx,))
        return cur.fetchone()[0]



    # def _create_rowbustable(self):
    #     """
    #         create a schema like table with prefix dunder to avoid name conflict
    #     """
    #     #conn = sqlite3.connect("{}.db".format(self.name))
    #     cur = self.__conn.cursor()
    #     cur.execute("SELECT EXISTS (SELECT 1 FROM sqlite_master WHERE type='table' AND name='rowbusta')")
    #     ret = cur.fetchone()[0]
    #     if  ret == 0:
    #         cur.execute("CREATE TABLE rowbusta (id INTEGER, name TEXT)")
    #     cur.close()
    #     # conn.close()

    # def _add_table(self, tablename):
    #     cur = self.__conn.cursor()
    #     # sql= "INSERT INTO __myrb (id, name) VALUES ({}, {});".format(self.get_tableid(tablename),tablename)
    #     # print(sql)
    #     cur.execute("INSERT OR IGNORE INTO rowbusta (id, name) VALUES (:id, :name);",{"id":self.get_tableid(tablename), "name":tablename})
    #     self.__conn.commit()
    #     cur.close()
        
    # def _create_table(self, name):
    #     cur = self.__conn.cursor()
    #     cur.execute("SELECT EXISTS (SELECT 1 FROM sqlite_master WHERE type='table' AND name=:tablename)", {"tablename":name})
    #     ret = cur.fetchone()[0]
    #     if  ret == 0:
    #         cur.execute("CREATE TABLE '{}' (id INTEGER PRIMARY KEY AUTOINCREMENT, cells JSON)".format(name))
    #         cur.execute("CREATE TABLE '__columns_{}' (id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL, name TEXT)".format(name))
    #     cur.close()    


    # def create_temp_table(self,name,columnnames = None):
    #     """a table that only lives in caller scope"""
    #     table = Table(name) # create a table
    #     if columnnames is not None:
    #         for columnname in columnnames: # add column
    #             table.add_column(columnname)
    #     return table   