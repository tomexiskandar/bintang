from openpyxl import load_workbook
import os
import json
import copy
from bintang.table import Table
from bintang import travdict
from pathlib import Path
import logging


log = logging.getLogger(__name__)
FORMAT = "[%(filename)s:%(lineno)s - %(funcName)10s() ] %(message)s"
logging.basicConfig(format=FORMAT)
log.setLevel(logging.ERROR)


class Bintang():
    def __init__(self, name=None, backend=None):
        self.name = name
        self.__tables = {} # this must be a dict of id:table object
        self.__last_assigned_tableid= -1 #
        self.__be = None
        if backend is not None:
            from bintang.besqlite import Besqlite
            self.__be = Besqlite(self.name)
 

    def __getitem__(self, tablename): # subscriptable version of self.get_table()
        tableid = self.get_tableid(tablename)
        return self.__tables[tableid]       

    def __repr__(self):
        rb = {}
        rb['name'] = self.name
        table = []
        for tablename in self.get_tablenames():
            table.append(tablename)
        rb['tables'] = table
        return json.dumps(rb, indent=2)

    

    def __del__(self):
        if self.__be is not None:
            self.__be.conn.close() #win need this otherwise a PermissionError: [WinError 32] ...
            os.remove(self.__be.dbpath)
        #self.__conn.close()
        # 
    def copy_db(self, dest=None):
        import shutil
        if dest == None:
            dest = os.getcwd()
        shutil.copy(self.__be.dbpath, dest)    


    def create_table(self, name, columnnames=None):
        table = Table(name) # create a table
        self.add_table(table)
        if self.__be is not None:   # if is_persistent is True then update the table attributes and pass the connection
            table._Table__be = self.__be           
            table._Table__be.add_table(self.get_tableid(name), name)
        if columnnames is not None:
            for columnname in columnnames: # add column
                table.add_column(columnname)
        
        
    def get_tableid(self, tablename):
        for id, table in self.__tables.items():
            if tablename.upper() == table.name.upper(): # compare uppercased to ensure case insensitivity
                return id
        return None


    def get_tablenames(self):
        tablenames = []
        for table in self.__tables.values():
            tablename = table.name
            tablenames.append(tablename)    
        return tablenames

    
    def get_columnnames(self, tablename):
        return self.get_table(tablename).get_columnnames()


    # def get_columnids(self, tablename, columnnames):
    #     tableid = self.get_tableid(tablename)
    #     return self.__tables[tableid].get_columnids
    

    def add_table(self,table):
        tableid = self.get_tableid(table.name)
        if tableid is None:
            tableid = self.__last_assigned_tableid + 1
            self.__tables[tableid] = table
            self.__last_assigned_tableid = self.__last_assigned_tableid + 1 # update it
        elif tableid is not None:
            raise ValueError('Table {} already exists.'.format(table.name))


    def rename_table(self, tablename, new_tablename):
        # check if tablename already exists
        if tablename in self.get_tablenames():
            for id, table in self.__tables.items():
                if tablename == table.name:
                    self.__tables[id].name = new_tablename
        else:
            raise ValueError('Tablename {} does not exist.'.format(tablename))

    def get_table(self, tablename):
        tableid = self.get_tableid(tablename)
        return self.__tables[tableid]


    def copy_table(self,source_tablename, destination_tablename):
        destination_table = copy.deepcopy(self.get_table(source_tablename))
        destination_table.name = destination_tablename
        self.add_table(destination_table)
        
    
    def drop_column(self,tablename,columnname):
        self.__tables[tablename].drop_column(columnname)


    def insert(self,tablename,columnnames,values):
        self.get_table(tablename).insert(columnnames,values)
        

    def get_cells_bycolumnnames(self,tablename,row,columnnames):
        _cells = {} # to hold the results cells
        for name in columnnames:
            columnid = self.__tables[tablename].get_columnid(name)
            # only assign if columnid exists to avoid a KeyError
            if columnid in row.cells:
                _cells[columnid] = row.cells[columnid]
        return _cells

    def delete_row(self, tablename, index):
        self.get_table(tablename).delete_row(index)

    
    def delete_rows(self, tablename, indexes):
        self.get_table(tablename).delete_rows(indexes)


    def print(self,tablename, top=None, columnnames=None):
        if columnnames is None:
            columnnames = [x.name for x in self.__tables[tablename].columns.values()]
        print(','.join(str(val) for val in columnnames))
        columnids = self.__tables[tablename].get_columnids(columnnames)
        
        # print each row values
        for idx,row in enumerate(self.__tables[tablename].rows.values()):
            row_values = row.get_values(columnids)
            print(','.join(str(val) for val in row_values))
            if idx == top:
                break

    def raise_valueerror_tablename(self,tablename):
        tableid = self.get_tableid(tablename)
        if tableid is None:
            raise ValueError('Tablename {} does not exist.'.format(tablename))


    def iterrows(self, tablename, columnnames=None, result_as = 'dict', rowid=False):
        """get the table form the collection
        then yield idx and row from table's iterrows()
        """
        self.raise_valueerror_tablename(tablename)
        for idx, row in self.get_table(tablename).iterrows(columnnames, result_as=result_as, rowid=rowid):
            yield idx, row
            

    def dev_read_db(self, connstr, tablename, columnnames = None):
        self.create_table(tablename)
        self[tablename].connstr = connstr
        import pyodbc
        conn = pyodbc.connect(connstr)
        cursor = conn.cursor()
        for row in cursor.tables():
            log.debug(row.table_name)
        if cursor.tables(table=tablename.lower()).fetchone():
            log.debug('table {} exists!'.format(tablename))
        else:
            log.debug('table {} not found'.format(tablename))
            # create table for rows
            sql_ = 'CREATE TABLE {} (idx serial NOT NULL PRIMARY KEY, id serial NOT NULL, cells json)'.format(tablename.lower())
            cursor.execute(sql_)
            cursor.commit()
            # create table for columns
            sql_ = 'CREATE TABLE {} (id serial NOT NULL PRIMARY KEY, name varchar(150))'.format(tablename.lower()+'_columns')
            cursor.execute(sql_)
            cursor.commit()
            # add columnnames if passed
            if columnnames is not None:
                if len(columnnames) > 0:
                    for colname in columnnames:
                        sql_ = "INSERT INTO {} (name) VALUES ('{}');".format(tablename.lower()+'_columns', colname)
                        log.debug(sql_)
                        cursor.execute(sql_)
                        cursor.commit()
        cursor.close()
        conn.close()    


    def dev_read_sqlite(self, tablename, columnnames = None):
        self.create_table(tablename)
        import sqlite3
        conn = sqlite3.connect('bintang.db')
        cur = conn.cursor()
        sql_ = "SELECT EXISTS (SELECT 1 FROM sqlite_master WHERE type='table' AND name='{}')".format(tablename)
        ret = cur.execute(sql_)
        for row in cur.execute('SELECT * FROM sqlite_master'):
            log.debug(row)


        conn.close()    
        quit()
        if True:
            log.debug('table {} exists!'.format(tablename))
        else:
            log.debug('table {} not found'.format(tablename))
            # create table for rows
            sql_ = 'CREATE TABLE {} (id serial NOT NULL, cells json)'.format(tablename.lower())
            cursor.execute(sql_)
            # create table for columns
            sql_ = 'CREATE TABLE {} (id serial NOT NULL PRIMARY KEY, name varchar(150))'.format(tablename.lower()+'_columns')
            cursor.execute(sql_)
            # add columnnames if passed
            if columnnames is not None:
                if len(columnnames) > 0:
                    for colname in columnnames:
                        sql_ = "INSERT INTO {} (name) VALUES ('{}');".format(tablename.lower()+'_columns', colname)
                        log.debug(sql_)
                        cursor.execute(sql_)
        cursor.close()
        conn.close() 


    def read_excel(self, path, sheetname):
        self.create_table(sheetname)        
        if self.__be is not None:
            self.__be.create_table(sheetname)
        wb = load_workbook(path, read_only=True, data_only=True)
        ws = wb[sheetname]
        columnnames = []
        for rownum, row_cells in enumerate(ws.iter_rows(),start=1):
            values = [] # hold column value for each row
            if rownum == 1:
                for cell in row_cells:
                    columnnames.append(cell.value)
            if None in columnnames:
                log.debug('columnnames: {}'.format(columnnames))
                log.error('Error! None column detected!')
                quit()    
            if rownum > 1:
                for cell in row_cells:
                    values.append(cell.value)
                self.get_table(sheetname).insert(columnnames, values)
        if self.__be is not None:
            self.get_table(sheetname).add_row_into_be()



    def read_dict(self, jsondata, tablepaths=[]):
        debug = False
        for row in travdict.traverse_dict(jsondata,tablepaths):
            # if debug:
            #     print("\n---------------------in bintang---------------------")
            #     print(row)
            
            for k,v in row.cells.items():
                # print(k,'->',v)
                
                # create a table if not created yet
                if row.tablepath not in self.get_tablenames():
                    self.create_table(row.tablepath)
                
                # upsert this row
                self.get_table(row.tablepath).upsert_jsonrow(row)
            # if debug:
            #     print("---------------------out bintang--------------------")


    def VOID_get_row_asdict(self, tablename, idx, columnnames=None):
        return self.get_table(tablename).get_row_asdict(idx, columnnames=columnnames)


    def _add_lcell(self, lidx, ltablename, outrow, output_tablename, output_lcolumnnames, rowid):
        for k, v in self[ltablename].get_row_asdict(lidx,rowid=rowid).items():
            if output_lcolumnnames is None:
                # incude all columns
                cell = self[output_tablename].make_cell(k,v)
                outrow.add_cell(cell)
            if output_lcolumnnames is not None:
                # include only the passed columns
                if k in output_lcolumnnames:
                    cell = self[output_tablename].make_cell(k,v)
                    outrow.add_cell(cell)
        return outrow            


    def _add_rcell(self, ridx, rtablename, outrow, output_tablename, output_rcolumnnames, rcolnames_resolved, rowid):
        for k, v in self[rtablename].get_row_asdict(ridx,rowid=rowid).items():
            if output_rcolumnnames is None:
                cell = self[output_tablename].make_cell(rcolnames_resolved[k],v)
                outrow.add_cell(cell)
            if output_rcolumnnames is not None:
                # include only the passed columns
                if k in output_rcolumnnames:
                    cell = self[output_tablename].make_cell(rcolnames_resolved[k],v)
                    outrow.add_cell(cell)
        return outrow


    def _resolve_join_columnnames (self,ltablename, rtablename, rowid=False):
        # resolve any conflict columnname by prefixing its tablename
        # notes: conflict columnname occurs when the same columnname being used in two joining table.
        rcolnames_resolved = {}
        if rowid:
            rcolnames_resolved['rowid_'] = ltablename + '_' + 'rowid_'
        lcolnames = self[ltablename].get_columnnames()
        for colname in self[rtablename].get_columnnames():
            if colname in lcolnames:
                rcolnames_resolved[colname] = rtablename + '_' + colname
            else:
                rcolnames_resolved[colname] = colname
        return rcolnames_resolved  



    # def _DEPRECATED_innerjoin(self,ltablename, lkeys
    #             ,rtablename, rkeys
    #             ,into
    #             ,output_lcolumnnames=None
    #             ,output_rcolumnnames=None
    #             ,rowid=False):

    #     # validate input eg. columnname etc
    #     lkeys = self[ltablename].validate_columnnames(lkeys)
    #     rkeys = self[rtablename].validate_columnnames(rkeys)
    #     if output_lcolumnnames is not None:
    #         output_lcolumnnames = self[ltablename].validate_columnnames(output_lcolumnnames)
    #     if output_rcolumnnames is not None:
    #         output_rcolumnnames = self[rtablename].validate_columnnames(output_rcolumnnames)

    #     # resolve columnnames conflicts
    #     rcolnames_resolved = self._resolve_join_columnnames(ltablename, rtablename, rowid)
    #     # create an output table
    #     output_tablename = into
    #     self.create_table(output_tablename)
    #     output_table = self.get_table(output_tablename)

    #     # for debuging create merged table to store the matching rowids
    #     #merged = self.create_table("merged",["lrowid","rrowid"])

    #     numof_keys = len(lkeys)
    #     # loop left table
    #     for lidx, lrow in self.iterrows(ltablename, columnnames=lkeys, rowid=rowid):
    #         # loop right table
    #         for ridx, rrow in self.iterrows(rtablename, columnnames=rkeys, rowid=rowid):
    #             matches = 0 # store matches for each rrow
    #             # compare value for any matching keys, if TRUE then increment matches
    #             for i in range(numof_keys):
    #                 if lrow[lkeys[i]] == rrow[rkeys[i]]:
    #                     matches += 1 # incremented!
    #             if matches == numof_keys: # if fully matched, create the row & add into the output table
    #                 #debug merged.insert(["lrowid","rrowid"], [lrow["_rowid"], rrow["_rowid"]])
    #                 #and add the row to output table
    #                 outrow = output_table.make_row()
    #                 # add cells from left table
    #                 outrow = self._add_lcell(lidx, ltablename, outrow, output_tablename, output_lcolumnnames, rowid)
    #                 # add cells from right table
    #                 outrow = self._add_rcell(ridx, rtablename, outrow, output_tablename, output_rcolumnnames, rcolnames_resolved, rowid)   
    #                 output_table.add_row(outrow)
    #     #debug merged.print() 
    #     return output_table


    def innerjoin(self
                ,ltablename: str #, lkeys
                ,rtablename: str #, rkeys
                ,on: list
                ,into: str
                ,output_lcolumnnames: list=None
                ,output_rcolumnnames: list=None
                ,rowid=False) -> Table:

        # validate input eg. columnname etc
        lkeys = [x[0] for x in on] # generate lkeys from on (1st sequence)
        rkeys = [x[1] for x in on] # generate rkeys from on (2nd sequence)
        lkeys = self[ltablename].validate_columnnames(lkeys)
        rkeys = self[rtablename].validate_columnnames(rkeys)
        if output_lcolumnnames is not None:
            output_lcolumnnames = self[ltablename].validate_columnnames(output_lcolumnnames)
        if output_rcolumnnames is not None:
            output_rcolumnnames = self[rtablename].validate_columnnames(output_rcolumnnames)

        # resolve columnnames conflicts
        rcolnames_resolved = self._resolve_join_columnnames(ltablename, rtablename, rowid)
        # create an output table
        output_tablename = into
        self.create_table(output_tablename)
        output_table = self.get_table(output_tablename)

        # for debuging create merged table to store the matching rowids
        #merged = self.create_table("merged",["lrowid","rrowid"])

        numof_keys = len(on) #(lkeys)
        # loop left table
        for lidx, lrow in self.iterrows(ltablename, columnnames=lkeys, rowid=rowid):
            # loop right table
            for ridx, rrow in self.iterrows(rtablename, columnnames=rkeys, rowid=rowid):
                matches = 0 # store matches for each rrow
                # compare value for any matching keys, if TRUE then increment matches
                for i in range(numof_keys):
                    #if lrow[on[0]] == rrow[on[1]]:
                    if lrow[lkeys[i]] == rrow[rkeys[i]]:
                        matches += 1 # incremented!
                if matches == numof_keys: # if fully matched, create the row & add into the output table
                    #debug merged.insert(["lrowid","rrowid"], [lrow["_rowid"], rrow["_rowid"]])
                    #and add the row to output table
                    outrow = output_table.make_row()
                    # add cells from left table
                    outrow = self._add_lcell(lidx, ltablename, outrow, output_tablename, output_lcolumnnames, rowid)
                    # add cells from right table
                    outrow = self._add_rcell(ridx, rtablename, outrow, output_tablename, output_rcolumnnames, rcolnames_resolved, rowid)   
                    output_table.add_row(outrow)
        #debug merged.print() 
        return output_table

    def leftjoin(self,ltablename, rtablename, lkeys, rkeys
                ,output_lcolumnnames=None
                ,output_rcolumnnames=None
                ,rowid=False):

        # validate input eg. columnname etc
        lkeys = self[ltablename].validate_columnnames(lkeys)
        rkeys = self[rtablename].validate_columnnames(rkeys)
        if output_lcolumnnames is not None:
            output_lcolumnnames = self[ltablename].validate_columnnames(output_lcolumnnames)
        if output_rcolumnnames is not None:
            output_rcolumnnames = self[rtablename].validate_columnnames(output_rcolumnnames)
        

        # create an output table 
        output_tablename = ltablename + rtablename
        self.create_table(output_tablename)
        output_table = self.get_table(output_tablename)

        # for debuging create merged table to store the matching rowids
        #merged = self.create_table("merged",["lrowid","rrowid"])

        numof_keys = len(lkeys)
        for lidx, lrow in self.iterrows(ltablename, columnnames=lkeys, rowid=rowid):
            outrow = output_table.make_row()
            # add cells for ltable
            outrow = self._add_lcell(lidx, ltablename, outrow, output_tablename, output_lcolumnnames, rowid)
            for ridx, rrow in self.iterrows(rtablename, columnnames=rkeys, rowid=rowid):
                matches = 0 # store matches for each rrow
                # evaluate any matching keys, if so increment matches
                for i in range(numof_keys):
                    if lrow[lkeys[i]] == rrow[rkeys[i]]:
                        matches += 1 # increment
                if matches == numof_keys: # if fully matched, create the row & add into the output table
                    #debug merged.insert(["lrowid","rrowid"], [lrow["_rowid"], rrow["_rowid"]])
                    # add cells for rtable
                    outrow = self._add_rcell(ridx, rtablename, outrow, output_tablename, output_rcolumnnames, ltablename, rowid)   
            output_table.add_row(outrow)             
        #debug merged.print() 
        return output_table