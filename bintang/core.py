import os
import json
import copy
import unicodedata
from difflib import SequenceMatcher
from bintang.table import Table, Table_Path
from bintang import iterdict
from pathlib import Path
from bintang.log import log
FUZZY_LIB = 'DIFFLIB'
try: 
    from rapidfuzz import fuzz , process, utils
    FUZZY_LIB = 'RAPIDFUZZ'
except ImportError as e:
    pass




# import logging

# log = logging.getLogger(__name__)
# FORMAT = "[%(filename)s:%(lineno)s - %(funcName)10s() ] %(message)s"
# logging.basicConfig(format=FORMAT)
# log.setLevel(logging.DEBUG)
# class TableNotFoundError(Exception):
#     def __init__(self,tablename):
#         tablenames = self.get_tables()
#         suggest = process.extract(tablename, tablenames, limit=2)
#         print('hello',suggest)
#         self.message = "Cannot find table '{}'.".format(tablename)
#         super().__init__(self.message)

def match_case(value1, value2):
    "Ascii case sensitive matching"
    if value1 == value2:
        return True
    

def match_caseless(value1, value2):
    "Ascii ase insensitive matching"
    if isinstance(value1, str) and isinstance(value2, str):
        if value1.lower() == value2.lower():
            return True
    elif isinstance(value1, str) or isinstance(value2, str):
        if str(value1) == str(value2):
            return True    
    else:
        if value1 == value2:
            return True    



def _normalize_caseless(string):
    return unicodedata.normalize("NFKD", string.casefold())


def match(value1, value2):
    """Unicode case insensitive matching.
    the ultimate goal is to get result regardless data type"""
    if isinstance(value1, str) and isinstance(value2, str):
        if _normalize_caseless(value1) == _normalize_caseless(value2):
            return True
    elif isinstance(value1, str) or isinstance(value2, str): # 1 vs '1' is True
        if str(value1) == str(value2):
            return True
    else:
        if value1 == value2:
            return True  
    
def get_similar_values(value, similar_values, min_ratio=0.6):
        # use standard difflib SequenceMatcher
        res = []
        for col in similar_values:
            ratio = SequenceMatcher(None, col.lower(), value.lower()).ratio()
            if ratio >= min_ratio:
                res.append((col,ratio))
        res_sorted = sorted(res, key=lambda tup: tup[1], reverse=True)
        return [x[0] for x in res_sorted] # just extract the name, not ratio

def get_diff_ratio(value1, value2, default_process=True):
    """ get string similarity between two values.
        It'll use rapidfuzz package if installed, otherwise python difflib will be used.
    """
    if value1 is None or value2 is None:
        return 0
        
    if FUZZY_LIB == 'RAPIDFUZZ' and default_process == True:
        return  round(fuzz.ratio(value1, value2, processor=utils.default_process), 2)
    elif FUZZY_LIB == 'RAPIDFUZZ' and default_process == False:
        return  round(fuzz.ratio(value1, value2), 2)
    elif FUZZY_LIB == 'DIFFLIB' and default_process == True:
        value1 = default_stringify(value1)
        value2 = default_stringify(value2)
        return round(SequenceMatcher(None, value1, value2).ratio() * 100, 2)
    else:
        return round(SequenceMatcher(None, value1, value2).ratio() * 100, 2)

def default_stringify(value):
    if isinstance(value, str):
        value = value.strip()
        value =unicodedata.normalize("NFKD", value.casefold()) #experiment
        return value.lower()
    else:
        return str(value)
    

    
def get_wb_type_toread(wb):
    """
        get workbook type so read_excel() know which func/attrb to use.
    """
    if str(type(wb)) == "<class 'openpyxl.workbook.workbook.Workbook'>":
        return 'openpyxl'
    elif str(type(wb)) == "<class 'xlrd.book.Book'>":
        return 'xlrd'
    else:
        raise ValueError('Sorry, for reading excel file, only openpyxl or xlrd workbook accepted!')
    

def get_wb_type_towrite(wb):
    """
        get workbook type so to_excel() know which func/attrb to use.
    """
    if str(type(wb)) == "<class 'openpyxl.workbook.workbook.Workbook'>":
        return 'openpyxl'
    elif str(type(wb)) == "<class 'xlwt.Workbook.Workbook'>":
        return 'xlwt'
    else:
        raise ValueError('Sorry, for writing excel file, only openpyxl or xlwt workbook accepted!')


class Bintang():
    def __init__(self, name=None, backend=None):
        self.name = name
        self.parent = 'dad'
        self.__tables = {} # this must be a dict of id:table object
        self.__last_assigned_tableid= 0 #-1 #
        self.__be = None # will be deprecated
        if backend is not None: # will be deprecated
            from bintang.besqlite import Besqlite # will be deprecated
            self.__be = Besqlite(self.name) # will be deprecated
 

    def __getitem__(self, tablename: str) -> Table: # subscriptable version of self.get_table()
        tableid = self.get_tableid(tablename)
        if tableid is None:
            tablenames = self.get_tables()
            similar_tables = get_similar_values(tablename, tablenames)
            if len(similar_tables) > 0:
                raise ValueError ('could not find table {}. Did you mean {}?'.format(repr(tablename),' or '.join(similar_tables)))
            else:
                raise ValueError ('could not find table {}.'.format(repr(tablename)))
        else:
            return self.__tables[tableid]       

    def __repr__(self):
        rb = {}
        rb['name'] = self.name
        table = []
        for tablename in self.get_tables():
            table.append(tablename)
        rb['tables'] = table
        return json.dumps(rb, indent=2)

    

    def __del__(self):
        if self.__be is not None:
            self.__be.conn.close() #win need this otherwise a PermissionError: [WinError 32] ...
            #os.remove(self.__be.dbpath)
        

    def copy_db(self, dest=None):
        import shutil
        if dest == None:
            dest = os.getcwd()
        shutil.copy(self.__be.dbpath, dest)      


    def create_table(self, name: str, 
                     columns: list=None,
                     conn: str=None) -> Table:
        """ create a table under bintang object
        name: Name of the table
        columns: List of columns (optional)"""
        tobj = Table(name, bing=self, conn=conn) # create a tobj object
        self.add_table(tobj)
        if self.__be is not None:   # if is_persistent is True then update the tobj attributes and pass the connection
            tobj._Table__be = self.__be           
            tobj._Table__be.add_table(self.get_tableid(name), name)
        if columns is not None:
            for column in columns: # add column
                tobj.add_column(column)
        return tobj        

    def drop_table(self, name):
        tableid = self.get_tableid(name)
        if tableid is None:
            tables = self.get_tables()
            similar_tables = get_similar_values(name, tables)
            raise ValueError ('could not find table {}. Did you mean {}?'.format(repr(name),' or '.join(similar_tables)))
        else:
            del self.__tables[tableid]


    def create_path_table(self, name, columns=None):
        tobj = Table_Path(name, bing=self) # create a table object
        self.add_table(tobj)
        if self.__be is not None:   # if is_persistent is True then update the tobj attributes and pass the connection
            tobj._Table__be = self.__be           
            tobj._Table__be.add_table(self.get_tableid(name), name)
        if columns is not None:
            for column in columns: # add column
                tobj.add_column(column)            
        
        
    def get_tableid(self, tablename):
        for id, table in self.__tables.items():
            if tablename.upper() == table.name.upper(): # compare uppercased to ensure case insensitivity
                return id
        return None


    def get_tables(self):
        tables = []
        for table in self.__tables.values():
            tablename = table.name
            tables.append(tablename)    
        return tables


    def get_tablepaths(self):
        pathnames = []
        for table in self.__tables.values():
            pathname = table.path
            pathnames.append(pathname)    
        return pathnames    

    
    def get_columns(self, tablename):
        return self.get_table(tablename).get_columns()


    # def get_columnids(self, tablename, columns):
    #     tableid = self.get_tableid(tablename)
    #     return self.__tables[tableid].get_columnids
    

    def add_table(self,table: Table): 
        tableid = self.get_tableid(table.name)
        if tableid is None:
            tableid = self.__last_assigned_tableid + 1
            self.__tables[tableid] = table
            self.__last_assigned_tableid = self.__last_assigned_tableid + 1 # update it
        elif tableid is not None:
            raise ValueError('Table {} already exists.'.format(table.name))


    def rename_table(self, name, new_tablename):
        # check if name already exists
        if name in self.get_tables():
            for id, table in self.__tables.items():
                if name == table.name:
                    self.__tables[id].name = new_tablename
        else:
            raise ValueError('Tablename {} does not exist.'.format(name))

    def get_table(self, name):
        tableid = self.get_tableid(name)
        if tableid is not None:
            return self.__tables[tableid]
        else:
            return None


    def copy_table(self,source_tablename, destination_tablename):
        destination_table = copy.deepcopy(self.get_table(source_tablename))
        destination_table.name = destination_tablename
        self.add_table(destination_table)
        
    
    def drop_column(self,tablename,column):
        self.__tables[tablename].drop_column(column)


    def insert(self,tablename,columns,values):
        self.get_table(tablename).insert(columns,values)
        

    def _get_cells_bycolumns(self,tablename,row,columns):
        _cells = {} # to hold the results cells
        for name in columns:
            columnid = self.__tables[tablename].get_columnid(name)
            # only assign if columnid exists to avoid a KeyError
            if columnid in row.cells:
                _cells[columnid] = row.cells[columnid]
        return _cells

    def delete_row(self, tablename, index):
        self.get_table(tablename).delete_row(index)

    
    def delete_rows(self, tablename, indexes):
        self.get_table(tablename).delete_rows(indexes)


    def _print(self):
        tobj = Table('Columns Info')
        row_dict = {}
        for tab in self.get_tables():
            self[tab].set_data_props() # it sets 
            row_dict['table'] = tab
            for col in self[tab].get_columns():
                cobj = self[tab].get_column_object(col)
                #row_dict['id'] = cobj.id
                row_dict['column'] = cobj.name
                row_dict['ordinal_position'] = cobj.ordinal_position
                row_dict['data_type'] = cobj.data_type
                row_dict['column_size'] = cobj.column_size
                row_dict['decimal_digits'] = cobj.decimal_digits
                row_dict['data_props'] = cobj.data_props
                tobj.insert(row_dict)
        tobj.print()


    def print(self):
        tobj = Table('Tables')
        for tab in self.get_tables():
            row_dict = {}
            row_dict['Bintang'] = self.name
            row_dict['Table'] = tab
            tobj.insert(row_dict)
        tobj.print()    


    def raise_valueerror_tablename(self,tablename):
        tableid = self.get_tableid(tablename)
        if tableid is None:
            #tablenames = self.get_tables()
            # suggest = process.extract(tablename, tablenames, limit=2)
            # print('hello',suggest)
            # print('yes')
            # quit()
            raise ValueError('Tablename {} does not exist.'.format(tablename))


    def iterrows(self, table, columns=None, row_type = 'dict', rowid=False):
        """get the table form the collection
        then yield idx and row from table's iterrows()
        """
        
        #self.raise_valueerror_tablename(tablename)
        if isinstance(table, Table):
            for idx, row in table.iterrows(columns, row_type=row_type, rowid=rowid):
                yield idx, row
        else:
            for idx, row in self.get_table(table).iterrows(columns, row_type=row_type, rowid=rowid):
                yield idx, row
                

    def dev_read_db(self, connstr, tablename, columns = None):
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
            # add columns if passed
            if columns is not None:
                if len(columns) > 0:
                    for col in columns:
                        sql_ = "INSERT INTO {} (name) VALUES ('{}');".format(tablename.lower()+'_columns', col)
                        log.debug(sql_)
                        cursor.execute(sql_)
                        cursor.commit()
        cursor.close()
        conn.close()    


    def _dev_read_sqlite(self, tablename, columns = None):
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
            # add columns if passed
            if columns is not None:
                if len(columns) > 0:
                    for col in columns:
                        sql_ = "INSERT INTO {} (name) VALUES ('{}');".format(tablename.lower()+'_columns', col)
                        log.debug(sql_)
                        cursor.execute(sql_)
        cursor.close()
        conn.close() 

        
    def read_excel(self, wb, sheetnames=None):
        wb_type = get_wb_type_toread(wb)
        # validate sheetnames
        # sheetnames_lced = {x.lower(): x  for x in wb.sheetnames}
        # validate sheetname
        if wb_type == 'openpyxl':
            sheetnames_lced = {x.lower(): x  for x in wb.sheetnames}
        else: # assume xlrd
            sheetnames_lced = {x.lower(): x  for x in wb.sheet_names()}
        if sheetnames is not None: # user specify sheets
            for sheetname in sheetnames:
                if sheetname.lower() not in sheetnames_lced:
                    # get similar sheetname
                    if wb_type == 'openpyxl':
                        similar_sheetnames = get_similar_values(sheetname, wb.sheetnames)
                    else: # assume xlrd
                        similar_sheetnames = get_similar_values(sheetname, wb.sheet_names())
                    raise ValueError ('could not find sheetname {}. Did you mean {}?'.format(repr(sheetname),' or '.join(similar_sheetnames)))
                self.create_table(sheetname)
                self[sheetname].read_excel(wb, sheetname)
        else: # exctract all sheets and create all tables
            for ws in wb:
                sheetname = ws.title
                self.create_table(sheetname)
                self[sheetname].read_excel(wb, sheetname)


    def read_dict(self, dict_obj, tablepaths=None, root='/', path_sep='/'):
        debug = False
        for tprow in iterdict.iterdict(dict_obj, tablepaths=tablepaths, root=root, path_sep=path_sep):
            # if debug:
            #     print("\n---------------------in bintang---------------------")
            #     print(tprow)
            
            for k,v in tprow.cells.items():
                # print(k,'->',v)
                
                # create a table if not created yet
                if tprow.tablepath not in self.get_tables():
                    self.create_path_table(tprow.tablepath)
                
                # upsert this row
                self.get_table(tprow.tablepath).upsert_table_path_row(tprow)
            # if debug:
            #     print("---------------------out bintang--------------------")


    def read_json(self, json_str, tablepaths=None):
        dict_obj = json.loads(json_str) # parse json_str
        self.read_dict(dict_obj, tablepaths) 


    def set_child_tables(self):
        for tab1 in self.get_tablepaths():
            print('table name',tab1)
            # print(self[tab1].get_path_aslist())
            tab1_pathlist = self[tab1].get_path_aslist()
            for tab2 in self.get_tablepaths():
                tab2_pathlist = self[tab2].get_path_aslist()
                if len(tab2_pathlist) - len(tab1_pathlist) == 1:
                    # this is a possible child, not grand child
                    matches = 0
                    for i in range(len(tab1_pathlist)):
                        if tab1_pathlist[i] == tab2_pathlist[i]:
                            matches += 1
                    if matches == len(tab1_pathlist):
                        # this is a child
                        self[tab1].children.append(tab2)      


    def read_sql(self, conn, tablename, sql_str, params=None ):
        self.create_table(tablename)
        cursor = conn.cursor()
        if params is not None:
            cursor.execute(sql_str, params)
        else:
            cursor.execute(sql_str)
        columns = [col[0] for col in cursor.description]
        for row in cursor.fetchall():
            self.insert(tablename, row, columns)


    def _add_lcell(self, lidx, ltable, outrow, out_table, out_lcolumns, rowid):
        for k, v in self[ltable].get_row_asdict(lidx,rowid=rowid).items():
            if out_lcolumns is None:
                # incude all columns
                cell = out_table.make_cell(k,v)
                outrow.add_cell(cell)
            if out_lcolumns is not None:
                # include only the passed columns
                if k in out_lcolumns:
                    cell = out_table.make_cell(k,v)
                    outrow.add_cell(cell)
        return outrow            


    def _add_rcell(self, ridx, rtable, outrow, out_table, out_rcolumns, rcol_resolved, rowid):
        for k, v in rtable.get_row_asdict(ridx,rowid=rowid).items():
            if out_rcolumns is None:
                cell = out_table.make_cell(rcol_resolved[k],v)
                outrow.add_cell(cell)
            if out_rcolumns is not None:
                # include only the passed columns
                if k in out_rcolumns:
                    cell = out_table.make_cell(rcol_resolved[k],v)
                    outrow.add_cell(cell)
        return outrow


    def _resolve_join_columns (self,ltable, rtable_obj, rowid=False):
        # resolve any conflict column by prefixing its tablename
        # notes: conflict column occurs when the same column being used in two joining table.
        rcol_resolved = {}
        if rowid:
            rcol_resolved['rowid_'] = ltable + '_' + 'rowid_'
        lcolumns = self[ltable].get_columns()
        for col in rtable_obj.get_columns():
            if col in lcolumns:
                rcol_resolved[col] = rtable_obj.name + '_' + col
            else:
                rcol_resolved[col] = col
        return rcol_resolved  


    def innerjoin(self
                ,ltable: str #, lkeys
                ,rtable: str | Table #, rkeys
                ,on: list[tuple] # list of lkey & r key tuple
                ,into: str | None =None
                ,out_lcolumns: list | None = None
                ,out_rcolumns: list | None = None
                ,rowid=False) -> Table:
        
        rtable_obj = None
        if isinstance(rtable,Table):
            rtable_obj = rtable
            rtable = rtable.name
        else:
            rtable_obj = self[rtable]
        # validate input eg. column etc
        lkeys = [x[0] for x in on] # generate lkeys from on (1st sequence)
        rkeys = [x[1] for x in on] # generate rkeys from on (2nd sequence)
        lkeys = self[ltable].validate_columns(lkeys)
        #rkeys = self[rtable].validate_columns(rkeys)
        rkeys = rtable_obj.validate_columns(rkeys)
        if out_lcolumns is not None:
            out_lcolumns = self[ltable].validate_columns(out_lcolumns)
        if out_rcolumns is not None:
            # out_rcolumns = self[rtable].validate_columns(out_rcolumns)
            out_rcolumns = rtable_obj.validate_columns(out_rcolumns)

        # resolve columns conflicts
        rcol_resolved = self._resolve_join_columns(ltable, rtable_obj, rowid)
        # create an output table
        out_table = into if into is not None else 'innerjoin'
        out_tobj = Table(out_table)
        # self.create_table(out_table)
        # out_tobj = self.get_table(out_table)

        # for debuging create merged table to store the matching rowids
        #merged = self.create_table("merged",["lrowid","rrowid"])

        numof_keys = len(on) #(lkeys)
        # loop left table
        for lidx, lrow in self.iterrows(ltable, columns=lkeys, rowid=rowid):
            # loop right table
            for ridx, rrow in self.iterrows(rtable_obj, columns=rkeys, rowid=rowid):
                matches = 0 # store matches for each rrow
                # compare value for any matching keys, if TRUE then increment matches
                for i in range(numof_keys): 
                    if match(lrow[lkeys[i]], rrow[rkeys[i]]):
                        matches += 1 # incremented!
                if matches == numof_keys: # if fully matched, create the row & add into the output table
                    #debug merged.insert(["lrowid","rrowid"], [lrow["_rowid"], rrow["_rowid"]])
                    #and add the row to output table
                    outrow = out_tobj.make_row()
                    # add cells from left table
                    outrow = self._add_lcell(lidx, ltable, outrow, out_tobj, out_lcolumns, rowid)
                    # add cells from right table
                    outrow = self._add_rcell(ridx, rtable_obj, outrow, out_tobj, out_rcolumns, rcol_resolved, rowid)   
                    out_tobj.add_row(outrow)
        #debug merged.print() 
        return out_tobj
                

    def leftjoin(self,ltable: str
                ,rtable: str
                ,lkeys, rkeys
                ,out_lcolumns=None
                ,out_rcolumns=None
                ,rowid=False):

        # validate input eg. column etc
        lkeys = self[ltable].validate_columns(lkeys)
        rkeys = self[rtable].validate_columns(rkeys)
        if out_lcolumns is not None:
            out_lcolumns = self[ltable].validate_columns(out_lcolumns)
        if out_rcolumns is not None:
            out_rcolumns = self[rtable].validate_columns(out_rcolumns)
        

        # create an output table 
        out_table = ltable + rtable
        self.create_table(out_table)
        out_tobj = self.get_table(out_table)

        # for debuging create merged table to store the matching rowids
        #merged = self.create_table("merged",["lrowid","rrowid"])

        numof_keys = len(lkeys)
        for lidx, lrow in self.iterrows(ltable, columns=lkeys, rowid=rowid):
            outrow = out_tobj.make_row()
            # add cells for ltable
            outrow = self._add_lcell(lidx, ltable, outrow, out_table, out_lcolumns, rowid)
            for ridx, rrow in self.iterrows(rtable, columns=rkeys, rowid=rowid):
                matches = 0 # store matches for each rrow
                # evaluate any matching keys, if so increment matches
                for i in range(numof_keys):
                    if match(lrow[lkeys[i]] == rrow[rkeys[i]]):
                        matches += 1 # increment
                if matches == numof_keys: # if fully matched, create the row & add into the output table
                    #debug merged.insert(["lrowid","rrowid"], [lrow["_rowid"], rrow["_rowid"]])
                    # add cells for rtable
                    outrow = self._add_rcell(ridx, rtable, outrow, out_table, out_rcolumns, ltable, rowid)   
            out_tobj.add_row(outrow)             
        #debug merged.print() 
        return out_tobj
    

    def create_linked_table(self, name, conn, sql_str=None, params=None ):
        tobj = Table(name, bing=self) # create a tobj object
        tobj.type = 'FROMSQL'
        tobj.fromsql_conn = conn
        if sql_str is None:
            tobj.fromsql_str = "SELECT * FROM {}".format(name)
        else:
            tobj.fromsql_str = sql_str
        tobj.fromsql_params = params    
        self.add_table(tobj)
  


    