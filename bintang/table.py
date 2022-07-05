from bintang import column
from bintang.column import Column
from bintang.cell import Cell
from bintang.row import Row
import json
import sqlite3
import uuid
import re
import logging


log = logging.getLogger(__name__)
FORMAT = "[%(filename)s:%(lineno)s - %(funcName)10s() ] %(message)s"
logging.basicConfig(format=FORMAT)
log.setLevel(logging.ERROR)


class ColumnNotFoundError(Exception):
    def __init__(self,tablename, columnname):
        self.message = "Cannot find column '{}' in table {}.".format(columnname, tablename)
        super().__init__(self.message)


class Table(object):
    """Define a table object
       - provide columns to store a dictionary of column objects
       - provide rows to store a dictionary of row objects
    """
    def __init__(self,name):
        self.name = name
        self.__columns = {}
        self.__rows = {}
        self.__temprows = []
        self.__last_assigned_columnid= -1 #
        self.__last_assigned_rowid = -1
        self.__be = None
        

    
    def __getitem__(self, idx): # subscriptable version of self.get_row_asdict()
        #return self.__rows[idx] # bad design. a raw row isn't that useful at client code
        return self.get_row_asdict(idx)   

    def __setitem__(self, rowcell, value): # subscriptable version of self.update_row()
        # where arg rowcell is a tuple of an idx of rows & columnname  passed by client code
        # eg.  table_obj[idx,columnname] = value
        self.update_row(rowcell[0], rowcell[1], value)
        


    def __str__(self):
        tbl = {}
        tbl['table name'] = self.name
        columns = []
        res = {k:v.name for k, v in self.__columns.items() }
        for k,v in self.__columns.items():
            columns.append(dict(id=v.id, name=v.name, column_size=v.column_size, data_props=v.data_props))
        tbl['columns'] = columns

        # tbl_str += self.name + '\n'
        # for columnname in self.get_columnnames():
        #     tbl_str += "{} {} {}\n".format(' '*3,self.get_columnid(columnname), columnname)
        return json.dumps(tbl, indent=2)
   

        
        

    def add_column(self,name):
        # check if the passed name already exists
        columnid = self.get_columnid(name)
        if columnid is None:
            col = Column(name)
            col.id = self.__last_assigned_columnid + 1
            self.__columns[col.id] = col
            self.__last_assigned_columnid= self.__last_assigned_columnid + 1

    def _add_column(self,name):
        # check if the passed name already exists
        columnid = self._get_columnid(name)
        log.debug(columnid)
        if columnid is None:
            col = Column(name)
            col.id = self.__last_assigned_columnid + 1
            self.__columns[col.id] = col
            self.__last_assigned_columnid= self.__last_assigned_columnid + 1        


    def get_columnid(self,columnname):
        for id, column in self.__columns.items():
            # match the columnname case insensitive
            if column.get_name_uppercased() == columnname.upper():
                return id
        return None

    


    def get_columnids(self,columnnames=None):
        columnids = []
        if columnnames is None: # assume user want all available column ids
            return [id for id in self.__columns.keys()]
        for columnname in columnnames:
            columnid = self.get_columnid(columnname)
            if columnid is None:
                raise ValueError('Cannot find column name {}.'.format(columnname))
            else:
                columnids.append(columnid)
        return columnids

            
            


    def rename_columnname(self,columnname,new_columnname):
        for v in self.__columns.values():
            if v.name == columnname:
                v.name = new_columnname
                return

        

    def drop_column(self,columnname):
        # get columnid
        columnid = self.get_columnid(columnname)
        #provide warning if the passed columnname does not exist
        if columnid is None:
            log.warning("warning... trying to drop a non-existence column '{}'".format(columnname))
            return False
        # delete the cell from cell
        for row in self.__rows.values():
            row.cells.pop(columnid,None)
        # delete the column
        self.__columns.pop(columnid,None)


    def get_columnname(self,columnid):
        return self.__columns[columnid].name
        

    def get_columnnames(self, columnnames=None):
        return [x.name for x in self.__columns.values()]


    def _get_columnnames_lced(self, columnnames=None):
        return {x.name.lower(): x.name  for x in self.__columns.values()}


    def get_column_data_props(self, columnname):
        columnid = self.get_columnid(columnname)
        return self.__columns[columnid].data_props

 
    def validate_columnname(self, columnname):
        """return columnname as the one stored in table.columns"""
        if columnname.lower() in self._get_columnnames_lced().keys():
            return self._get_columnnames_lced().get(columnname.lower())
        else:
            raise ColumnNotFoundError(self.name, columnname)


    def validate_columnnames(self, columnnames):
        """return columnnames as ones stored in table.columns"""
        res = []
        for columnname in columnnames:
            if columnname.lower() in self._get_columnnames_lced().keys():
                res.append(self._get_columnnames_lced().get(columnname.lower()))
            else:
                raise ColumnNotFoundError(self.name, columnname)
        return res                


    def VOID_validate_columnnames(self,columnnames):
        res = []
        for columnname in columnnames:
            columnid = self.get_columnid(columnname)
            if columnid is not None:
                existing_columnname = self.get_columnname(columnid)
                res.append(existing_columnname)
            else:
                raise ValueError("Cannot find column {}.".format(columnname))
        return columnnames
        

    def make_row(self,id=None, option=None):
        """make a new row.
        by default it sets uuid4 for id.
        """
        if id is None and option is None:
            row = Row(self.__last_assigned_rowid + 1)
            self.__last_assigned_rowid += 1 #increment rowid
        elif id is not None and option is None:
            row = Row(id)
        elif id is None and option == 'uuid':
            row = Row(uuid.uuid4())
        return row


    def insert(self,columnnames,values):
        """ restrict arguments for data insertion for this function as the followings:
        1. a pair of columnnames and its single-row values
        2. a pair of columnnames and its multiple-row values (as a list of tuple)
        """

        if isinstance(values[0],tuple): # if no. 2
            for value in values:
                if isinstance(value,tuple): # if True then expect a multi-row insert. so add a row for each value
                    row = self.make_row()
                    for idx,columnname in enumerate(columnnames):
                        cell = self.make_cell(columnname,value[idx])
                        row.add_cell(cell)
                    # add to rows
                    self.add_row(row)    
        #else: # if no. 1return None
        elif isinstance(columnnames,list) and not isinstance(values[0],tuple):
            row = self.make_row()
            for idx, columnname in enumerate(columnnames):
                cell = self.make_cell(columnname,values[idx])
                row.add_cell(cell)
            # add to rows
            if self.__be is not None:
                self.__temprows.append(json.dumps({v.columnid: v.value for v in row.cells.values()}))
                if len(self.__temprows) == self.__be.max_row_for_sql_insert:
                    self.add_row_into_be()
            else:
                self.add_row(row)    
        else:
            raise ValueError("Insert only allows a pair of columns and values or columns and multiple row values (in tuple)")


    def add_row_into_be(self):
        self.__be.add_row(self.name, self.__temprows)
        self.__temprows.clear()      


    def insert_dict(self, adict):
        self.insert([x for x in adict.keys()], [x for x in adict.values()])


    def insert_todb(self,columnnames,values):
        """ restrict arguments for data insertion for this function as the followings:
        1. a pair of columnnames and its single-row values
        2. a pair of columnnames and its multiple-row values (as a list of tuple)
        """
        if isinstance(columnnames,list) and not isinstance(values[0],tuple):
            row = self.make_row()
            for idx, columnname in enumerate(columnnames):
                cell = self.make_cell(columnname,values[idx])
                row.add_cell(cell)
            # add to rows
            self.add_row(row)
        else:
            raise ValueError("Insert only allows a pair of columns and values or columns and multiple row values (in tuple)")

    def get_indexes(self):
        return [x for x in self.__rows.keys()]


    def get_rowidx_byrowid(self, rowid):
        debug = False
        if debug:
            print("\n  ------------------in get_row_rowid (table.py) --------------------")
        for idx, row in self.__rows.items():
            if debug:
                print(idx, row)
            if row.id == rowid:
                return idx
        if debug:
            print("\n  ------------------out get_row_rowid (table.py) -------------------")
        return None    


    def upsert_jsonrow(self, jsonrow):
        debug = False
        if debug:
            print("\n  ------------------in upsert_jsonrow (table.py) --------------------")
        # extract the rowid and use it as the table index (the key of rows{})
        # create a row if the rowid not found in the table's index
        res_idx = self.get_rowidx_byrowid(jsonrow.id)
        
        if res_idx is None:
            if debug:
                print("inserting... row does not exist", jsonrow)
            row = self.make_row(jsonrow.id)
            # re-make cells from jsonrow
            for id, c in jsonrow.cells.items():
                if debug:
                    print('cell:', id, ";", c)
                cell = self.make_cell(c.get_columnname(), c.value)
                row.add_cell(cell)
            # add to rows
            self.add_row(row)
            
        elif res_idx is not None:
            if debug:
                print("updating... row exists", jsonrow)
            for id, c in jsonrow.cells.items():
                cell = self.make_cell(c.get_columnname(), c.value)
                self.__rows[res_idx].add_cell(cell)        
        if debug:
            print("\n  ------------------out upsert_jsonrow (table.py)-------------------"    )


    def make_cell(self,columnname,value,new_columnname=True):
        columnid = self.get_columnid(columnname)
        if columnid is None: # if columnid is None then assume user wants a new column
            if new_columnname == True:
                self.add_column(columnname)
                columnid = self.get_columnid(columnname) # reassign the columnid
                if self.__be is not None:
                    self.__be.add_column(self.name, columnid, columnname)
                # deprecated moved up columnid = self.get_columnid(columnname) # reassign the columnid
        if columnid is None:
            raise ValueError("Cannot make cell due to None column name.")    
        return Cell(columnid,value)




    def add_row(self,row):
        rows_idx = len(self.__rows)
        self.__rows[rows_idx] = row

    def _XXgen_row_asdict(self, row, columnnames, rowid=False):
        res = {}
        if rowid == True:
            res['_rowid_'] = row.id # add rowid for internal purpose eg. a merged table
        for columnname in columnnames:
            columnid = self.get_columnid(columnname)
            if columnid not in row.cells:
                res[columnname] = None
            else:
                res[columnname] = row.cells[columnid].value
        return res   
    

    def delete_row(self,index):
        self.__rows.pop(index,None)


    def delete_rows(self,indexes):
        for index in indexes:
            self.delete_row(index)


    def get_row(self,idx):
        if idx in self.__rows:
            return self.__rows[idx]
        else:
            raise KeyError ('Cannot find index {}.'.format(idx))
    

    def get_row_asdict(self, idx, columnnames=None, rowid=False):
        if idx not in self.__rows:
            raise KeyError ('Cannot find index {}.'.format(idx))
        if idx in self.__rows:
            if columnnames is None:
                columnnames = self.get_columnnames()
            if columnnames is not None:
                columnnames = self.validate_columnnames(columnnames)
            return self._gen_row_asdict(self.__rows[idx],columnnames, rowid)
            

    def _gen_row_asdict(self, row, columnnames, rowid=False):
        res = {}
        if rowid == True:
            res['_rowid_'] = row.id # add rowid for internal purpose eg. a merged table
        for columnname in columnnames:
            columnid = self.get_columnid(columnname)
            if columnid not in row.cells:
                res[columnname] = None
            else:
                res[columnname] = row.cells[columnid].value
        return res


    def get_row_aslist(self, idx, columnnames=[]):
        if columnnames is None:
            columnnames = self.get_columnnames()
        columnids = self.get_columnids(columnnames)
        return self._gen_row_aslist(self.__rows[idx],columnids)


    def _gen_row_aslist(self, row, columnids):
        #columnids = self.get_columnids(columnnames)
        return row.get_values(columnids)

       
    def iterrows(self, columnnames=None, result_as='dict', rowid=False):
        if columnnames is None:
                columnnames = self.get_columnnames() # assign all available column names
        if result_as == 'dict': 
            if self.__be is None:
                for idx, row in self.__rows.items():
                    yield idx, self._gen_row_asdict(row,columnnames,rowid)
            if self.__be is not None:
                for idx, row in self.__be.iterrows_asdict(self.name, columnnames):
                    yield idx, row
        elif result_as == 'list':
            columnids = self.get_columnids(columnnames)
            for idx, row in self.__rows.items():
                yield idx, self._gen_row_aslist(row,columnids)


    def set_data_props(self):
        columnids = self.get_columnids()
        for idx, row in self.__rows.items():
            for columnid in columnids:
                log.debug(row)
                log.debug('id: {}, value: {}'.format(idx , self.__columns[columnid].name))
                if columnid in row.cells:
                    self.set_data_props_datatype(columnid, row.cells[columnid].value)
                    if row.cells[columnid].value is not None:
                        if type(row.cells[columnid].value).__name__ == 'str':
                            self.set_data_props_str_column_size(columnid, row.cells[columnid].value)


    def set_data_props_datatype(self, columnid,value):
        # set a column's data_type list
        if value is not None:
            if type(value).__name__ not in self.__columns[columnid].data_props:
                self.__columns[columnid].data_props[type(value).__name__] = dict(column_size=1)
                if type(value).__name__ == 'int':
                    self.__columns[columnid].data_props['int']['column_size'] = 10
                if type(value).__name__ == 'float':
                    self.__columns[columnid].data_props['float']['column_size'] = 15
                if type(value).__name__ == 'bool':
                    self.__columns[columnid].data_props['bool']['column_size'] = 1
                if type(value).__name__ == 'datetime':
                    self.__columns[columnid].data_props['datetime']['column_size'] = 19        

                


    def set_data_props_str_column_size(self, columnid, value):
        if len(value) > self.__columns[columnid].data_props['str']['column_size']:
            self.__columns[columnid].data_props['str']['column_size'] = len(value)       
    
        
    def print(self,top=None, columnnames=None):
        rows_values = []
        for enum,row in enumerate(self.__rows.values(),1):
            row_values = []
            for columnid in self.__columns.keys():
                cell_value = None
                if columnid in row.cells:
                    cell_value = row.cells[columnid].value
                row_values.append(cell_value)
            rows_values.append(row_values)
            if enum == top:
                break
        # get and print out columnnames
        columnnames = [x.name for x in self.__columns.values()]
        print(columnnames) 
        # get and print out rowresult_as
        for row in rows_values:
            print(row)


    def print_aslist(self, top=None, columnnames=None):
        print("idx",self.get_columnnames(columnnames))
        for idx, row in self.iterrows(result_as='list',columnnames=columnnames):
            print(idx, row)


    def print_asdict(self, top=None, columnnames=None):
        for idx, row in self.iterrows(columnnames=columnnames):
            print(idx, row)

    
    def are_columnnames_valid(self, columnnames):
        existing_columnnames = self.get_columnnames()
        for columnname in columnnames:
            if columnname not in existing_columnnames:
                raise ValueError("Cannot find column name {}.".format(columnname))
        return True


    def validate_stmt(self, stmt):
        invalid_keywords = ['import ']
        for ik in invalid_keywords:
            if ik in stmt:
                raise ValueError("Found invalid keyword {} in the statement!".format(repr(ik)))


    def update_stmt(self, row, stmt, columnnames):
        for columnname in columnnames:
            stmt = stmt.replace('`' + columnname + '`',repr(row[columnname]))
        return stmt


    def exec_stmt(self, stmt):
        # log.debug('-' * 10)
        # log.debug(stmt)
        res = {'retval':False}
        # res["__retval"] = None
        try:
            exec(stmt, globals(), res)
        except Exception as e:
            res["error"] = e
        finally:
            return res


    def get_index_exec(self, stmt):
        columnnames_in_stmt = re.findall('`(.*?)`',stmt) # extract column names from within a small tilde pair ``
        self.columnnames_valid(columnnames_in_stmt)  # validate column name from the stmt

        # scan the rows to search the index
        indexes = []
        for idx, row in self.iterrows():
            stmt_updated = self.update_stmt(row,stmt,columnnames_in_stmt)
            #self.validate_stmt(stmt)
            ret = self.exec_stmt(stmt_updated)
            if ret['retval'] == True:
                indexes.append(idx)
        return indexes


    def delete_rows_by_stmt(self, stmt):
        indexes = self.get_index_exec(stmt)
        self.delete_rows(indexes)


    def update_row(self,idx,columnname,value):
        self.__rows[idx].add_cell(self.make_cell(columnname,value))


    def update_row_all(self,columnname,value):
        for idx in self.__rows:
            self.__rows[idx].add_cell(self.make_cell(columnname,value))    


    def get_index_lambda(self,expr):
        indexes = []
        for idx, row in self.iterrows():
            #log.debug('idx row:',idx, row)
            idx = expr(idx,row)
            if idx is not None:
                indexes.append(idx)       
        return indexes


    # def get_type_used(self, columnname):
    #     # iterate through the table and suss out the type
    #     used_types = []
    #     for idx, row in self.iterrows(columnnames=[columnname]):
    #         #if row[columnname] is not None:
    #         used_type = type(row[columnname]).__name__
    #         if used_type not in used_types:
    #             used_types.append(used_type)
    #     return used_types


    # def get_types_used(self, columnname):
    #     # self.__columns[1].name = 'manuf'
    #     # for k,v in self.__columns.items():
    #     #     log.debug(k,v.name)
        
    #     # quit()
    #     # iterate through the table and suss out the type

    #     for idx, row in self.iterrows():
    #         #if row[columnname] is not None:
    #         used_type = type(row[columnname]).__name__
    #         if used_type not in used_types:
    #             used_types.append(used_type)
    #     return used_types    


    def tlookup(self, lktable, keys, lkkeys, return_lkcolumnnames, return_ascolumnnames = None):
        # validate input eg. columnname etc
        keys = self.validate_columnnames(keys)
        lkkeys = lktable.validate_columnnames(lkkeys)
        return_lkcolumnnames = lktable.validate_columnnames(return_lkcolumnnames)

        as_columnnames = {} # define a resolved columnnames, in case both table have a same columnname
        if return_ascolumnnames is None:
            for columnname in return_lkcolumnnames:
                if columnname in self.get_columnnames():
                    # prefix '_lk' if same name found
                    as_columnnames[columnname] = '_lk' + columnname
                else:
                    as_columnnames[columnname] = columnname
        if return_ascolumnnames is not None:
            as_columnnames = dict(zip(return_lkcolumnnames,return_ascolumnnames))


        numof_keys = len(keys)
        for idx, row in self.iterrows(keys, rowid=True):
            for lkidx, lkrow in lktable.iterrows():
                matches = 0
                for i in range(numof_keys):
                    if row[keys[i]] == lkrow[lkkeys[i]]:
                        matches += 1 # increment
                if matches == numof_keys:
                    # update this table row for each return_lkcolumnnames
                    for columnname in return_lkcolumnnames:
                        value = lktable[lkidx][columnname]
                        self.update_row(idx, as_columnnames[columnname], value)


    def to_excel(self, path, index = False):
        from openpyxl import Workbook
        wb = Workbook()
        ws = wb.active
        # add header
        columnnames = self.get_columnnames()
        log.debug(index)
        if index:
            if index == True:
                columnnames.insert(0,'_idx')
            if index != True:
                
                columnnames.insert(0,str(index))        
        ws.append(columnnames)
        # add row
        if index:
            for idx, row in self.iterrows(result_as='list'):
                row.insert(0,idx)
                ws.append(row)
        if not index:
            for idx, row in self.iterrows(result_as='list'):
                ws.append(row)          
        wb.save(path)

