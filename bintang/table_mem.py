import json
import bintang
from bintang.table import Base_Table
from bintang.column import Column
from bintang.cell import Cell
from bintang.row import Row
from bintang.log import log
import types
from operator import itemgetter

class Memory_Table(Base_Table):
    """Define a Bintang table object
       - provide columns to store a dictionary of column objects
       - provide rows to store a dictionary of row objects
    """
    def __init__(self,name, bing=None, conn=None):
        super().__init__(name, bing=bing)
        self.__columns = {}
        self.__rows = {}
        self.__temprows = []
        self.__last_assigned_columnid= 9 #
        self.__last_assigned_rowid = 0 # for use when row created
        self.__last_assigned_idx = 0 # for use when add idx
        
    def __getitem__(self, idx): # subscriptable version of self.get_row_asdict()
        #return self.__rows[idx] # bad design. a raw row isn't that useful at client code
        return self.get_row_asdict(idx)


    def __setitem__(self, rowcell, value): # subscriptable version of self.update_row()
        # where arg rowcell is a tuple of an idx of rows & column  passed by client code
        # client code signature: table_obj[idx,column] = value
        self.update_row(rowcell[0], rowcell[1], value)    
        

    def __repr__(self):
        tbl = {}
        tbl['name'] = self.name
        columns = []
        for k,v in self.__columns.items():
            columns.append(dict(id=v.id, name=v.name))
        tbl['columns'] = columns
        return json.dumps(tbl, indent=2)


    def __len__(self):
        """ return the length of rows"""
        # if self.conn is not None:
        #     cursor = self.conn.cursor()
        #     res = cursor.execute(f'SELECT COUNT(*) FROM {self.name};')
        #     return res.fetchone()[0]
        return len(self.__rows)


    def get_size(self):
        return sys.getsizeof(self.__rows)
        
 
    def set_to_sql_colmap(self, columns):
        if isinstance(columns, list) or isinstance(columns, tuple):
            return dict(zip(columns, columns))
        elif isinstance(columns, dict):
            return columns

        
    # here the location of def_to_sql()    
    
    def _to_sql_upsert_dev(self, conn: str,
                    schema: str,
                    table: str,
                    columns: list[str],
                    on: list[tuple], # keys to match btw bintang and sql
                    method='prep') -> None:
        
        for row in self._iterrow_sql(conn):
            print(row)


    def _iterrow_sql(self, conn, sql_str=None, params=None):
        cursor = conn.cursor()
        if sql_str is None:
            sql_str = "SELECT * FROM {}".format(self.name)
        if params is not None:
            cursor.execute(sql_str, params)
        else:
            cursor.execute(sql_str)
        columns = [col[0] for col in cursor.description]
        for row in cursor.fetchall():
            yield dict(zip(columns, row))

    
    def gen_sql_stmt_create_table_dev(self, dbms, schema = None, str_size = None):
        # scanning table to get column properties
        # and assign the data type and size (when able)
        self.set_data_props()
        for col in self.get_columns():
            cobj = self.__columns[self.get_columnid(col)]
            if cobj.data_type is None:
                if len(cobj.data_props) == 0: # a column that has no data at all
                    cobj.data_type = 'str'  # force to str
                    cobj.column_size = str_size # force to str_size
                # loop through the data props for column that has it
                if 'str' in cobj.data_props:
                    cobj.data_type = 'str'
                    if str_size: # if user size to be used
                        if dbms == 'sqlserver':
                            if cobj.data_props['str']['column_size'] > 1:
                                cobj.column_size = str_size
                            else:
                                cobj.column_size = cobj.data_props['str']['column_size']
                    else:    
                        cobj.column_size = cobj.data_props['str']['column_size']
                    # loop through any other type if the column_size bigger, if it is assign it.
                    # for k, v in cobj.data_props.items():
                    #     if v['column_size'] > col_size:
                    #         col_size = v['column_size']
                    # cobj.column_size = col_size #cobj.data_props['str']['column_size']
                elif 'datetime' in cobj.data_props:
                    cobj.data_type = 'datetime'
                    cobj.column_size = cobj.data_props['datetime']['column_size']
                else: # just get the first one and break. to be observed later on!
                    for type, prop in cobj.data_props.items():
                        cobj.data_type = type
                        break
        
        # use type_map to translate the type
        create_columns = []
        for col in self.get_columns():
            cobj = self.__columns[self.get_columnid(col)]
            colname = cobj.name
            dtype = self.type_map[dbms]['type_mappings'][cobj.data_type]
            di_start = self.type_map[dbms]['delimited_identifiers']['start']
            di_end = self.type_map[dbms]['delimited_identifiers']['end']
            if cobj.data_type == 'str':
                col_size = cobj.column_size if cobj.column_size <= 4000 else 'max'
                create_item = [f'{di_start}{colname}{di_end}', f'{dtype}', f'({col_size})']
                create_columns.append(create_item)
            else:
                create_item = [f'{di_start}{colname}{di_end}', f'{dtype}']
                create_columns.append(create_item)       
        create_columns_str = []
        for i, citem in enumerate(create_columns):
            create_item_str = []
            if i == 0: # add tab if first, the rest will be added at later join
                create_item_str.append('\t')
            for i in citem:
                create_item_str.append(i)
            create_item_str.append('\n')
            create_columns_str.append(' '.join(create_item_str))
            schema_name = '' if schema is None else schema + '.'
        create_sqltable_templ = 'CREATE TABLE {}"{}" (\n{})'.format(schema_name, self.name, '\t,'.join(create_columns_str))
        return create_sqltable_templ 
    

    def add_column(self, name, data_type=None, column_size=None):
        # if self.type == 'MEMORY':

        # check if the passed name already exists
        columnid = self.get_columnid(name)
        if columnid is None:
            cobj = Column(name)
            if data_type is not None:
                cobj.data_type = data_type
            if column_size is not None:
                cobj.column_size = column_size
            cobj.id = self.__last_assigned_columnid + 1
            cobj.ordinal_position = self.__last_assigned_columnid + 1
            self.__columns[cobj.id] = cobj
            self.__last_assigned_columnid= self.__last_assigned_columnid + 1
        else:
            log.debug(f'Warning! trying to add existing column "{name}".')
        
        # else:
        #     log.debug(f'Warning! add_columns() only alowed for Memory table.')


    def update_column(self,name, data_type=None, column_size=None, ordinal_position=None):
        # check if the passed name already exists
        columnid = self.get_columnid(name)
        if columnid is not None:
            if data_type is not None:
                self.__columns[columnid].data_type = data_type
            if column_size is not None:
                self.__columns[columnid].column_size = column_size
            if ordinal_position is not None:
                self.__columns[columnid].ordinal_position = ordinal_position
          

    def get_columnid(self,column):
        # if self.conn is None:
        for id, cobj in self.__columns.items():
            # match the column case insensitive
            if cobj.get_name_uppercased() == column.upper():
                return id
        # return None
        # else: # assume conn
        #     return self._get_columnid_sql(column)      

    
    def get_columnids(self,columns=None):
        columnids = []
        if columns is None: # assume user want all available column ids
            return [id for id in self.__columns.keys()]
        for column in columns:
            columnid = self.get_columnid(column)
            if columnid is None:
                raise ValueError('Cannot find column name {}.'.format(column))
            else:
                columnids.append(columnid)
        return columnids

            
    def rename_column(self,old_column,new_column):
        for v in self.__columns.values():
            if v.name == old_column:
                v.name = new_column
                return

        
    def drop_column(self,column):
        # get columnid
        columnid = self.get_columnid(column)
        #if self.conn is None:
            #provide warning if the passed column does not exist
        if columnid is None:
            log.warning("warning... trying to drop a non-existence column '{}'".format(name))
            return False
        # delete the cell from cell. Let's revisit this, Q: do we need to delete the data coz it's not linked to existing column since the the colum dropped.
        for row in self.__rows.values():
            row.cells.pop(columnid,None)
        # delete the column
        self.__columns.pop(columnid,None)
        # else:
        #     columnid = self._get_columnid_sql(column)
        #     log.debug(columnid)
            
        #     sql = 'DELETE FROM __columns__ WHERE id = ?;'
        #     cursor = self.conn.cursor()
        #     cursor.execute(sql, (columnid,))
        #     self.conn.commit()


#  sql = 'SELECT id FROM __columns__ where name = ?'
#         cursor = self.conn.cursor()
#         res = cursor.execute(sql,(column,))
#         ret = res.fetchone()
#         if ret:
#             return ret['id']
#         else:
#             return None
        


    def get_column(self,columnid):
        return self.__columns[columnid].name
    

    def order_columns(self, columns):
        # determine all columns
        columns_all = [x for x in columns]
        for col in self.get_columns():
            if col not in columns_all:
                columns_all.append(col)
        for i, col in enumerate(columns_all, 10):
            self.update_column(col, ordinal_position=i)


    def _order_columns_sql(self, columns):
        # determine all columns
        columns_all = [x for x in columns]
        for col in self._get_columns_sql():
            if col not in columns_all:
                columns_all.append(col)
        for i, col in enumerate(columns_all, 10):
            self._update_column_sql(col, ordinal_position=i)        


    def get_columns(self) -> tuple:
        # if self.type == 'SQLBACKEND':#conn is not None:
        #     sorted_columns = self._get_columns_sql()
        # elif self.type == 'FROMSQL':
        #     sorted_columns = self._get_columns_fromsql()
        # else:
            # DEP as not sorted wise return [x.name for x in self.__columns.values()]
        col_objs = [col for col in self.__columns.values()]
        col_objs.sort(key=lambda col: col.ordinal_position)
        sorted_columns = [col.name for col in col_objs]
        return tuple(sorted_columns)   


    def _get_columnnames_lced_XXX(self, columns=None) -> dict:
        # if self.type == 'SQLBACKEND':
        #     pass
        # elif self.type == 'FROMSQL':
        #     # execute sql
        #     # cursor = self.fromsql_conn.cursor()
        #     # cursor.execute(self.fromsql_str)
        #     # columns_fromsql = [col[0] for col in cursor.description]
        #     # return {x.name.lower(): x.name  for x in columns_fromsql}
        #     pass
        # else:
        #return {x.name.lower(): x.name  for x in self.__columns.values()}
        return {col.lower(): col  for col in self.get_columns()}


    def get_data_props(self, column):
        columnid = self.get_columnid(column)
        return self.__columns[columnid].data_props


    def get_column_object(self, column):
        columnid = self.get_columnid(column)
        return self.__columns[columnid]

 
    def validate_column(self, column):
        """return column as the one stored in table.columns"""
        if column.lower() in self._get_columnnames_lced().keys():
            return self._get_columnnames_lced().get(column.lower())
        else:
            similar_cols = bintang.core.get_similar_values(column, self.get_columns())
            raise ValueError ('could not find column {}. Did you mean {}?'.format(repr(column),' or '.join(similar_cols)))


    def copy_index(self, column='idx',at_start=False):
        for idx in self.__rows:
            if isinstance(idx, tuple): # if index generated from a groupby
                self.update_row(idx, column, str(idx))
            else:
                self.update_row(idx, column, idx)
        if at_start:
            self.order_columns([column])


    def get_index(self, condition_column, condition_value):
        """will return the first index which value matches the condition"""
        for idx, row in self.iterrows():
            # test if string
            # if type(row[condition_columnname])==str:pass
            if row[condition_column] == condition_value:
                return idx

    def get_indexes(self, condition_column, condition_value):
        """will return a list of indexes which value matches the condition"""
        indexes = []
        for idx, row in self.iterrows():
            # test if string
            # if type(row[condition_columnname])==str:pass
            if row[condition_column] == condition_value:
                indexes.append(idx)
        return indexes    


    def get_value(self, column, where = None):
        """ 
        return a scalar value.
        switch different process if user want column pass as string or lambda
        params:
        column: either column name or using lambda expression
        where: always a lambda expression"""
        if where is not None:
            if isinstance(column,str):
                for idx, row in self.iterrows():
                    if where(row):
                        return row[column]
            elif isinstance(column, types.FunctionType):
                for idx, row in self.iterrows():
                    if where(row):
                        return column(row)
            else:
                raise 'param column must be either column name or lambda and param where must be a lambda.'
        else:
            if isinstance(column,str):
                for idx, row in self.iterrows():
                    return row[column]
            elif isinstance(column, types.FunctionType):
                for idx, row in self.iterrows():
                    return column(row)
            else:
                raise 'param column must be either column name or lambda and param where must be a lambda.'


    def index_exists(self, index):
        if index in self.__rows:
            return True
        else:
            return False
        


    def make_row(self,id=None, option=None):
        """make a new row.
        by default it increments id.
        """
        if id is None:
            row = Row(self.__last_assigned_rowid + 1)
            self.__last_assigned_rowid += 1 #increment rowid
        elif id is not None:
            row = Row(id)
        # elif id is None and option == 'uuid':
        #     row = Row(uuid.uuid4())
        return row


    def insert(self, record, columns=None, index=None):
        """ restrict arguments for record insertion for this function as the followings:
        1. a dictionary pass to record param
        2. list values pass to record param and list columns pass to column param.
        """
        if isinstance(record, dict):
            row = self.make_row()
            # if self.conn is None:
            for idx, (col, val) in enumerate(record.items()):
                cell = self.make_cell(col,val)
                row.add_cell(cell) # add to row
            self.add_row(row, index)   
            
                                                  
        elif isinstance(columns,list) or isinstance(columns,tuple) or isinstance(record,list) or isinstance(record,tuple):
            row = self.make_row()
            # if self.conn is None:
            for idx, col in enumerate(columns):
                cell = self.make_cell(col,record[idx])
                row.add_cell(cell) # add to rows
            self.add_row(row, index) 
        else:
            raise ValueError("Arg for record set for dictionary or list/tuple of values with list/tuple of columns.")
        

    def _insert(self, record, columns=None, index=None):
        """ restrict arguments for record insertion for this function as the followings:
        1. a pair of columns and its single-row values
        deprecated2. a pair of columns and its multiple-row values (as a list of tuple)
        """
        if isinstance(record, dict):
            row = self.make_row()
            for idx, (col, val) in enumerate(record.items()):
                cell = self.make_cell(col,val)
                row.add_cell(cell) # add to row
            self.add_row(row, index)                                    
        elif isinstance(columns,list) or isinstance(columns,tuple) or isinstance(record,list) or isinstance(record,tuple):
            row = self.make_row()
            for idx, col in enumerate(columns):
                cell = self.make_cell(col,record[idx])
                row.add_cell(cell) # add to rows
            if self.__be is not None:
                self.__temprows.append(json.dumps({v.columnid: v.value for v in row.cells.values()}))
                if len(self.__temprows) == self.__be.MAX_ROW_SQL_INSERT:
                    self.add_row_into_be()
            else:
                self.add_row(row, index)    
        else:
            raise ValueError("Arg for record set for dictionary or list/tuple of values with list/tuple of columns.")    
            
            
    def add_row_into_be(self):
        self.__be.add_row(self.name, self.__temprows)
        self.__temprows.clear()      


    def _insert_dict(self, adict):
        self.insert([x for x in adict.keys()], [x for x in adict.values()])


    def insert_todb(self,columns,values):
        """ restrict arguments for data insertion for this function as the followings:
        1. a pair of columns and its single-row values
        2. a pair of columns and its multiple-row values (as a list of tuple)
        """
        if isinstance(columns,list) and not isinstance(values[0],tuple):
            row = self.make_row()
            for idx, column in enumerate(columns):
                cell = self.make_cell(column,values[idx])
                row.add_cell(cell)
            # add to rows
            self.add_row(row)
        else:
            raise ValueError("Insert only allows a pair of columns and values in a list or tuple")


    def get_rowidx_byrowid(self, rowid):
        debug = False
        # if debug:
        #     print("\n  ------------------in get_row_rowid (table.py) --------------------")
        for idx, row in self.__rows.items():
            if debug:
                print(idx, row)
            if row.id == rowid:
                return idx
        # if debug:
        #     print("\n  ------------------out get_row_rowid (table.py) -------------------")
        return None    


    def upsert_table_path_row_moved(self, tprow):
        #log.debug("\n  ------------------in upsert_table_path_row (table.py) --------------------")
        # extract the rowid and use it as the table index (the key of rows{})
        # create a row if the rowid not found in the table's index
        res_idx = self.get_rowidx_byrowid(tprow.id)
        
        if res_idx is None:
            #log.debug(f'inserting... row does not exist {tprow}')
            row = self.make_row(tprow.id)
            # re-make cells from tprow
            for id, c in tprow.cells.items():
                #log.debug(f'{id} cell: {c}')
                if c.is_key == True:
                    cell = self.make_cell(self.PARENT_PREFIX + c.get_column(), c.value)
                else:
                    cell = self.make_cell(c.get_column(), c.value)

                row.add_cell(cell)
            # add to rows
            self.add_row(row)
            
        elif res_idx is not None:
            #log.debug(f"updating... row exists {tprow}")
            for id, c in tprow.cells.items():
                if c.is_key == True:
                    cell = self.make_cell(self.PARENT_PREFIX + c.get_column(), c.value)
                else:
                    cell = self.make_cell(c.get_column(), c.value)
                self.__rows[res_idx].add_cell(cell)        
        #log.debug("\n  ------------------out upsert_table_path_row (table.py)-------------------")


    def make_cell(self,column,value,new_column=True):
        columnid = self.get_columnid(column)
        if columnid is None: # if columnid is None then assume user wants a new column
            if new_column == True:
                self.add_column(column)
                columnid = self.get_columnid(column) # reassign the columnid
        if columnid is None:
            raise ValueError("Cannot make cell due to None column name.")    
        return Cell(columnid,value)    


    def __deprecated_add_row(self,row):
        rows_idx = len(self.__rows) # can cause re-assign a deleted row
        self.__rows[rows_idx] = row

    def __deprecated_add_row_OLD(self, row):
        rows_idx = self.__last_assigned_idx + 1
        self.__last_assigned_idx += 1
        self.__rows[rows_idx] = row


    def add_row(self, row, index=None):
        if index is None:
            index = self.__last_assigned_idx + 1
            self.__last_assigned_idx += 1
        self.__rows[index] = row      


    def delete(self, where):
        # get all index
        indexes = [x for x in self.__rows]
        for idx in indexes:
            try:
                if where(self.get_row(idx)):
                    self.delete_row(idx)
            except:
                pass


    def delete_row(self,index):
        self.__rows.pop(index,None)


    def delete_rows(self,indexes):
        for index in indexes:
            self.delete_row(index)


    def _get_row(self,idx):
        if idx in self.__rows:
            return self.__rows[idx]
        else:
            raise KeyError ('Cannot find index {}.'.format(idx))


    def get_valid_columnname(self, column):
        columnid = self.get_columnid(column)   # column in this line passed by user
        column = self.get_column(columnid) # ensure the same column passed as result.
        return column


    def get_row(self, index, columns=None, rowid=False, row_type='dict'):
        if row_type.lower()=='list':
            return self.get_row_aslist(index, columns)
        else:
            return self.get_row_asdict(index, columns, rowid)
           

    def get_row_asdict(self, idx, columns=None, rowid=False):
        if idx not in self.__rows:
            # DEPRECATED raise KeyError ('Cannot find index {}.'.format(idx))
            return None
        if idx in self.__rows:
            if columns is not None:
                columns = self.validate_columns(columns)
            else:
                columns = self.get_columns()
            return self._gen_row_asdict(self.__rows[idx],columns, rowid)


    def _get_row_sql(self, idx, columns=None, row_type='dict'):
        cursor = self.conn.cursor()
        sql = 'SELECT cells from {} WHERE idx=?'.format(self.name)
        params = [idx]
        cursor = self.conn.cursor()
        cursor.execute(sql, params)
        res = cursor.fetchone()
        if res:
            if columns is None:
                columns = self._get_columns_sql()
            if row_type.lower() == 'list':
                return [x for x in self._gen_row_dict_sql(res['cells'], columns).values()]
            else:
                return self._gen_row_dict_sql(res['cells'], columns)
            

    def _gen_row_asdict(self, row, columns, rowid=False):
        res = {}
        if rowid == True:
            res['rowid_'] = row.id # add rowid for internal purpose eg. a merged table
        for column in columns:
            # get valid column. these two lines defined in def get_valid_columnname()
            columnid = self.get_columnid(column)   # column in this line passed by user
            column = self.get_column(columnid) # ensure the same column passed as result.
            if columnid not in row.cells:
                res[column] = None
            else:
                res[column] = row.cells[columnid].value       
        return res


    def get_row_aslist(self, idx, columns=None):
        if columns is None:
            columns = self.get_columns()
        columnids = self.get_columnids(columns)
        return self._gen_row_aslist(self.__rows[idx],columnids)


    def _gen_row_aslist(self, row, columnids):
        return row.get_values(columnids)


    def iterrows(self, 
                 columns: list=None, 
                 row_type: str='dict', 
                 where=None, 
                 rowid: bool=False):
        
        # need to refactor this funct ion:
        # the main structure should be based on table type!!!

        # validate user's args
        if columns is not None:
            columns = self.validate_columns(columns)
                  

        if columns is None:
            columns = self.get_columns() # assign all available column names
            
        if row_type == 'list':
            columnids = self.get_columnids(columns)
            for idx, row in self.__rows.items():
                yield idx, self._gen_row_aslist(row,columnids)

        else: # assume row_type is dict
            if where is not None:
                for idx, row in self.__rows.items():
                    if where(self._gen_row_asdict(row, columns, rowid)):
                        yield idx, self._gen_row_asdict(row,columns,rowid)
            else:
                for idx, row in self.__rows.items():
                    yield idx, self._gen_row_asdict(row,columns,rowid)
      
    
    
    def set_data_props(self):
        """ scan table to obtain columns properties - data type, column size (if str type then the max of len of string)"""
        columnids = self.get_columnids()
        columns = self.get_columns()
        for idx, row in self.__rows.items():
            for columnid in columnids:
                # log.debug(row)
                # log.debug('id: {}, value: {}'.format(idx , self.__columns[columnid].name))
                if columnid in row.cells:
                    self.set_data_props_datatype(columnid, row.cells[columnid].value)
                    if row.cells[columnid].value is not None:
                        if type(row.cells[columnid].value).__name__ == 'str':
                            self.set_data_props_str_column_size(columnid, row.cells[columnid].value)
                        if type(row.cells[columnid].value).__name__ not in ['str','int','float','bool','datetime']:
                            self.set_data_props_other_column_size(columnid, row.cells[columnid].value)


    def set_data_props_datatype(self, columnid,value):
        # set a column's data_type
        # set predefined column size for known data type, int, float, bool, datetime
        # while for str and the rest, need to go through all column cells and get greatest length.
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


    def set_data_props_other_column_size(self, columnid, value):
        if len(str(value)) > self.__columns[columnid].data_props[type(value).__name__]['column_size']:
            self.__columns[columnid].data_props[type(value).__name__]['column_size'] = len(str(value))         


    def print_columns_info(self):
        self.set_data_props()
        dp_tobj = Memory_Table(f'{self.name} - Columns Info')
        row_dict = {}
        for col in self.get_columns():
            cobj = self.get_column_object(col)
            row_dict['id'] = cobj.id
            row_dict['name'] = cobj.name
            row_dict['ordinal_position'] = cobj.ordinal_position
            row_dict['data_type'] = cobj.data_type
            row_dict['column_size'] = cobj.column_size
            row_dict['decimal_digits'] = cobj.decimal_digits
            row_dict['data_props'] = cobj.data_props
            dp_tobj.insert(row_dict)
        dp_tobj.print()

    
    
    def _get_index_column_size(self):
        column_size = 0
        for idx in self.__rows:
            # print(idx)
            # print(len(''.join(idx)))
            if len(str(idx)) > column_size:
                column_size = len(str(idx))
        return max(len('idx'),column_size)
    

    def _gen_print_line(self, length, col_div_pos, square=False):
        line_chars = []
        if square:
            for x in range(length):
                if x in [0,length-1]:
                    line_chars.append('+')
                elif x  in col_div_pos:
                    line_chars.append('+')
                else:
                    line_chars.append('-')
        else:
            for x in range(length):
                if x  in col_div_pos:
                    line_chars.append('+')
                else:
                    line_chars.append('-')
        return ''.join(line_chars)

    def _gen_heading_col_str(self, heading_col, square=False):
        # heading_col_str = '|'.join(heading_col)
        if square:
            temp_heading_col = list('|'.join(heading_col))
            temp_heading_col[0] = '|'
            temp_heading_col[-1] = '|'
            return ''.join(temp_heading_col)
        else:
            return '|'.join(heading_col)

    def _gen_print_padded_cells_str(self, padded_cells, square=False):
        if square:
            temp_cells = list('|'.join(padded_cells))
            temp_cells[0] = '|'
            temp_cells[-1] = '|'
            # print(temp_cells)
            # quit()
            return ''.join(temp_cells)
        else:
            return '|'.join(padded_cells)


        

    def print(self, columns=None, show_data_type=False, square=False):
        # validate columns arg
        if columns: #user want specific columns
            columns_ = self.validate_columns(columns)
        else:
            columns_ = self.get_columns()
            
        PAD_SIZE = 4
        self.set_data_props() # generate data based type and column_size
        # generate heading_col string
        header_data_types = []
        heading_col = []
        col_div_pos = [] #[idx_max_width]
        for col in columns_: #self.get_columns():
            cobj = self.get_column_object(col)
            _data_types_str = ','.join(cobj.get_data_types())
            dp = cobj.get_greatest_column_size_data_prop()
            max_width = PAD_SIZE # initial value
            if dp is None:
                max_width = max(4, len(col)) + PAD_SIZE
            else:
                max_width = max(next(iter(dp.values()))['column_size'],len(col)) + PAD_SIZE
            if len(col_div_pos)==0:
                col_div_pos.append(max_width)
            else:    
                col_divider_pos = col_div_pos[-1] + max_width + 1
                col_div_pos.append(col_divider_pos)

            if show_data_type:
                header_dt = _data_types_str.center(max_width)
                header_data_types.append(header_dt)
            header_col = col.center(max_width)
            heading_col.append(header_col)

        if show_data_type:
            header_data_types_str = '|'.join(header_data_types)
        heading_col_str = self._gen_heading_col_str(heading_col, square=square)
  
        print(('Table: ' + self.name).center(len(heading_col_str))) # print title
        if show_data_type:
            print(header_data_types_str) # print header data type
        print(self._gen_print_line(len(heading_col_str), col_div_pos,  square=square))
        print(heading_col_str) # print heading_col str
        print(self._gen_print_line(len(heading_col_str), col_div_pos,  square=square))
        
        # generate row string
        for idx, row in self.iterrows(columns_):
            padded_cells = []
            for col, val in row.items():
                cobj = self.get_column_object(col)
                dp = cobj.get_greatest_column_size_data_prop()
                max_width = PAD_SIZE # initial value
                if dp is None:
                    max_width = max(4, len(col)) + PAD_SIZE
                else:
                    max_width = max(next(iter(dp.values()))['column_size'],len(col)) + PAD_SIZE
                # if type(val).__name__ not in ['str','int','float','bool','datetime','NoneType']:
                #     padded_cells.append((' ' + str(type(val).__name__) + ' obj').rjust(max_width))
                if cobj.has_string_data_type():
                   padded_cells.append(('  ' + str(val)).ljust(max_width)) #add double spaces for readable
                else:
                    padded_cells.append((str(val) + '  ').rjust(max_width)) # add double spaces for readable
            #padded_cells_str = '|'.join(padded_cells)
            print(self._gen_print_padded_cells_str(padded_cells,  square=square))
        print(self._gen_print_line(len(heading_col_str), col_div_pos,  square=square))
        print('({} rows)'.format(len(self)))    

    
    def __deprecated_columnnames_valid(self, columns):
        existing_columnnames = self._get_columnnames_lced()
        for column in columns:
            if column.lower() not in existing_columnnames:
                raise ValueError("Cannot find column name {}.".format(column))
        return True


    def __deprecated_replace_insensitively(self, old, repl, text):
        return re.sub('(?i)'+re.escape(old), lambda m: repl, text)

    def __deprecated_validate_stmt(self, stmt):
        invalid_keywords = ['import ']
        for ik in invalid_keywords:
            if ik in stmt:
                raise ValueError("Found invalid keyword {} in the statement!".format(repr(ik)))


    def __deprecated_set_cellvalue_stmt(self, row, stmt):
        columns = re.findall('`(.*?)`',stmt) # extract column names from within a small tilde pair ``
        for column in columns:
            stmt = stmt.replace('`' + column + '`',repr(row[column]))
        return stmt


    def __deprecated_exec_stmt(self, stmt):
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


    def __deprecated_get_index_exec(self, stmt):
        """return the fist index that matches the condition."""
        columnname_case_insensitive = False
        if columnname_case_insensitive == True:
            columnnames_in_stmt = re.findall('`(.*?)`',stmt) # extract column names from within a small tilde pair ``
            self.columnnames_valid(columnnames_in_stmt)  # validate column name from the stmt
            columnnames_in_stmt = [self.get_valid_columnname(x) for x in columnnames_in_stmt]
            for column in columnnames_in_stmt:
                valid_columnname = self.get_valid_columnname(column)
                stmt = self.replace_insensitively(column, valid_columnname, stmt)
        # scan the rows to search the index
        indexes = []
        for idx, row in self.iterrows():
            stmt_set = self.set_cellvalue_stmt(row, stmt)
            self.validate_stmt(stmt_set)
            ret = self.exec_stmt(stmt_set)
            if ret['retval'] == True:
                return idx
        

    def __deprecated_get_indexes_exec(self, stmt):
        """return a list of indexes that match the condition."""
        columnname_case_insensitive = False
        if columnname_case_insensitive == True:
            columnnames_in_stmt = re.findall('`(.*?)`',stmt) # extract column names from within a small tilde pair ``
            self.columnnames_valid(columnnames_in_stmt)  # validate column name from the stmt
            columnnames_in_stmt = [self.get_valid_columnname(x) for x in columnnames_in_stmt]
            for column in columnnames_in_stmt:
                valid_columnname = self.get_valid_columnname(column)
                stmt = self.replace_insensitively(column, valid_columnname, stmt)
        # scan the rows to search the index
        indexes = []
        for idx, row in self.iterrows():
            stmt_set = self.set_cellvalue_stmt(row, stmt)
            self.validate_stmt(stmt_set)
            ret = self.exec_stmt(stmt_set)
            if ret['retval'] == True:
                indexes.append(idx)
        return indexes


    def __deprecated_delete_rows_by_stmt(self, stmt):
        indexes = self.get_index_exec(stmt)
        self.delete_rows(indexes)


    def update_row(self,idx,column,value):
        if idx in self.__rows:
            self.__rows[idx].add_cell(self.make_cell(column,value))
        else:
            fn = inspect.stack()[0][3]
            warnings.warn(f'Warning! {fn}() trying to update a non existed row (ie. idx {idx}).')

   
    def update_row_all(self,column,value):
        for idx in self.__rows:
            self.__rows[idx].add_cell(self.make_cell(column,value))    


    def exists(self, where):
        for idx, row in self.iterrows():
            try:
                if where(row):
                    return True
            except:
                print(row)
                pass
        return False    
        
        
    def update(self, column, value, where=None):
        if isinstance(value, types.FunctionType):
            # value passed as function
            for idx, row in self.iterrows():
                if where is None:
                    try:
                        val = value(row) # function called
                        self.update_row(idx, column, val)
                    except Exception as e:
                        #log.warning(e)
                        pass    
                else:    
                    try:
                        if where(row):
                            val = value(row) # function called
                            self.update_row(idx, column, val) 
                    except Exception as e:
                        #log.warning(e)
                        pass 
        else:
            # value passed as non function eg. an int or str
            for idx, row in self.iterrows():
                if where is None:
                    self.update_row(idx, column, value)
                else:    
                    try:
                        if where(row): # function called 
                            self.update_row(idx, column, value) 
                    except Exception as e:
                        # print(e)
                        pass 



    
    def get_index_lambda(self,expr):
        for idx, row in self.iterrows():
            log.debug('idx {} row{}:'.format(idx, row))
            ret = expr(row)
            if ret:
                return idx


    def get_indexes_lambda(self,expr):
        indexes = []
        for idx, row in self.iterrows():
            #log.debug('idx {} row{}:'.format(idx, row))
            ret = expr(row)
            if ret:
                indexes.append(idx)       
        return indexes


    def filter(self, 
                columns: list=None, 
                where: callable=None):
        tobj = Memory_Table('filtered')
        if columns is None: # all columns
            columns = self.get_columns()
        else:
            columns = self.validate_columns(columns)
        for idx, row in self.iterrows(columns):
            if where:
                try:
                    if where(row):
                        tobj.insert(row)
                except Exception as e:
                    pass
                    # log.debug(f'false at {idx}')
                    # log.warning(e)    
            else:
                tobj.insert(row)
        return tobj


    def innerjoin(self
                ,rtable # either string or Table object
                ,on: list[tuple] # list of lkey & r key tuple
                ,into: str
                ,out_lcolumns: list=None
                ,out_rcolumns: list=None
                ,rowid=False):
        
        rtable_obj = None
        if isinstance(rtable,Memory_Table):
            rtable_obj = rtable
            rtable = rtable.name
        else:
            rtable_obj = self.bing[rtable]
        
        # validate input eg. column etc
        lkeys = [x[0] for x in on] # generate lkeys from on (1st sequence)
        rkeys = [x[1] for x in on] # generate rkeys from on (2nd sequence)
        lkeys = self.validate_columns(lkeys)
        #rkeys = self[rtable].validate_columns(rkeys)
        rkeys = rtable_obj.validate_columns(rkeys)
        if out_lcolumns is not None:
            out_lcolumns = self.validate_columns(out_lcolumns)
        if out_rcolumns is not None:
            # out_rcolumns = self[rtable].validate_columns(out_rcolumns)
            out_rcolumns = rtable_obj.validate_columns(out_rcolumns)

        # resolve columns conflicts
        rcol_resolved = self._resolve_join_columns(rtable_obj, rowid)
        # create an output table
        out_table = into
        out_tobj = self.bing.create_table(out_table)
        out_tobj = self.bing.get_table(out_table)
        
        # for debuging create merged table to store the matching rowids
        #merged = self.create_table("merged",["lrowid","rrowid"])

        numof_keys = len(on) #(lkeys)
        # loop left table
        for lidx, lrow in self.iterrows(columns=lkeys, rowid=rowid):
            # loop right table
            for ridx, rrow in rtable_obj.iterrows(columns=rkeys, rowid=rowid):
                matches = 0 # store matches for each rrow
                # compare value for any matching keys, if TRUE then increment matches
                for i in range(numof_keys): 
                    if bintang.match(lrow[lkeys[i]], rrow[rkeys[i]]):
                        matches += 1 # incremented!
                if matches == numof_keys: # if fully matched, create the row & add into the output table
                    #debug merged.insert(["lrowid","rrowid"], [lrow["_rowid"], rrow["_rowid"]])
                    #and add the row to output table
                    outrow = out_tobj.make_row()
                    # add cells from left table
                    log.debug(f'{lidx}')
                    outrow = self._add_lcell(lidx,  outrow, out_table, out_lcolumns, rowid)
                    # add cells from right table
                    outrow = self._add_rcell(ridx, rtable_obj, outrow, out_table, out_rcolumns, rcol_resolved, rowid)   
                    out_tobj.add_row(outrow)
        #debug merged.print() 
        return out_tobj


    def _resolve_join_columns (self, rtable_obj, rowid=False):
        # resolve any conflict column by prefixing its tablename
        # notes: conflict column occurs when the same column being used in two joining table.
        rcol_resolved = {}
        if rowid:
            rcol_resolved['rowid_'] = rtable_obj.name + '_' + 'rowid_'
        lcolumns = self.get_columns()
        for col in rtable_obj.get_columns():
            if col in lcolumns:
                rcol_resolved[col] = rtable_obj.name + '_' + col
            else:
                rcol_resolved[col] = col
        return rcol_resolved
    
    def _add_lcell(self, lidx, outrow, out_table, out_lcolumns, rowid):
        for k, v in self.get_row_asdict(lidx,rowid=rowid).items():
            if out_lcolumns is None:
                # incude all columns
                cell = self.bing[out_table].make_cell(k,v)
                outrow.add_cell(cell)
            if out_lcolumns is not None:
                # include only the passed columns
                if k in out_lcolumns:
                    cell = self.bing[out_table].make_cell(k,v)
                    outrow.add_cell(cell)
        return outrow            


    def _add_rcell(self, ridx, rtable, outrow, out_table, out_rcolumns, rcol_resolved, rowid):
        for k, v in rtable.get_row_asdict(ridx,rowid=rowid).items():
            if out_rcolumns is None:
                cell = self.bing[out_table].make_cell(rcol_resolved[k],v)
                outrow.add_cell(cell)
            if out_rcolumns is not None:
                # include only the passed columns
                if k in out_rcolumns:
                    cell = self.bing[out_table].make_cell(rcol_resolved[k],v)
                    outrow.add_cell(cell)
        return outrow
  

    def blookup(self, 
                lkp_table: object, 
                on: list[tuple], 
                ret_columns: list[str] | list[tuple]):
        lkp_table_obj = None
        if isinstance(lkp_table,Memory_Table):
            lkp_table_obj = lkp_table
            lkp_table = lkp_table.name
        else:
            lkp_table_obj = self.bing[lkp_table]
        
        # validate input eg. column etc
        lkeys = [x[0] for x in on] # generate lkeys from on (1st sequence)
        rkeys = [x[1] for x in on] # generate rkeys from on (2nd sequence)
        lkeys = self.validate_columns(lkeys)
        rkeys = lkp_table_obj.validate_columns(rkeys)
        lcolumn_prematch = self.get_columns() # will use this list when matching occurs below
        # validate ret_columns once:
        # otherwise will make lots of call later
        valid_ret_columns = []
        for item in ret_columns:
            if isinstance(item,str):
                item = lkp_table_obj.validate_column(item)
                valid_ret_columns.append(item)
            if isinstance(item, tuple):
                valid_column  = lkp_table_obj.validate_column(item[0])
                item_ = tuple([valid_column,item[1]])
                valid_ret_columns.append(item_)
        # add these valid_ret_column to the left table
        for col in valid_ret_columns:
            if isinstance(col, str):
                self.add_column(col)
            if isinstance(col, tuple):
                self.add_column(col[1]) # use the alias
        # for each matching, update it
        for lidx, ridx in self.scan(lkp_table, on=on, full=False):
            # update this table lrow for each ret_columns
            for item in valid_ret_columns:
                if isinstance(item, str): # if item is a column
                    # value = lkp_table_obj[ridx][item]
                    value = lkp_table_obj.get_row_asdict(ridx, columns=[item])[item]
                    if item in lcolumn_prematch:
                        # update left table with the lkp_table name as prefix
                        # this is to avoid column name conflict
                        self.update_row(lidx, self.bing[lkp_table].name + '_' + item, value)
                    else:
                        self.update_row(lidx, item, value)
                if isinstance(item, tuple): # if a tuple (0=column to return from lkp_table, 1=as_column)
                    value = lkp_table_obj[ridx][item[0]]
                    self.update_row(lidx, item[1], value)
    

    def scan(self, lkp_table: str, on: list[str] = None, full=True):
        for lidx, lrow in self.iterrows(columns=(col[0] for col in on), rowid=True):
            for ridx, rrow in self.bing[lkp_table].iterrows(col[1] for col in on):
                matches = 0
                for i in range(len(on)):
                    if bintang.match(lrow[on[i][0]], rrow[on[i][1]]):
                        matches += 1 
                if matches == len(on):
                    yield lidx, ridx
                    if not full:
                        break  # only return the first match


    def _scanfuzzy(self, lkeys, lkp_table_obj, rkeys, min_ratios):
        """ will yield two items. a left idx and result (a list of sorted matched right index and ratio tuple)
        for eg. yield 1, [(4, [80, 90]),(3, [70, 69])]
        where: 1 = the left index, 
               [(4, [80, 90]),(3, [70, 69])] = the sorted tuple list
        """
        lenof_keys = len(lkeys)
        for lidx, lrow in self.iterrows(lkeys, rowid=True):
            res_dict = {}
            for ridx, rrow in lkp_table_obj.iterrows():
                matches = 0
                ratios = []
                for i in range(lenof_keys):
                    ratio = bintang.get_diff_ratio(lrow[lkeys[i]], rrow[rkeys[i]])
                    if ratio >= min_ratios[i]:
                        matches += 1 
                        ratios.append(ratio)
                if matches == lenof_keys:
                    res_dict[ridx] = ratios
            
            if len(res_dict) > 1:
                res_tuples_sorted = sorted(res_dict.items(), key=itemgetter(1), reverse=True)
                yield lidx, res_tuples_sorted
            if len(res_dict) == 1:
                yield lidx, list(res_dict.items())


    def fuzzy_scan(self, lkp_table, on, full=True):
        """ will yield two items. a left idx and result (a list of sorted matched right index and ratio tuple)
        for eg. yield 1, [(4, [80, 90]),(3, [70, 69])]
        where: 1 = the left index, 
               [(4, [80, 90]),(3, [70, 69])] = the sorted tuple list
        """
        lenof_keys = len(on)
        for lidx, lrow in self.iterrows(columns=(c[0] for c in on)):
            res_dict = {}
            for ridx, rrow in self.bing[lkp_table].iterrows(columns=(c[1] for c in on)):
                matches = 0
                ratios = []
                for i in range(lenof_keys):
                    ratio = bintang.get_diff_ratio(lrow[on[i][0]], rrow[on[i][1]])
                    if ratio >= on[i][2]:
                        matches += 1 
                        ratios.append(ratio)
                if matches == lenof_keys:
                    res_dict[ridx] = ratios
                    yield lidx, res_dict
                    if not full:
                        break
                
            # print(res_dict)
            # if len(res_dict) > 1:
            #     res_tuples_sorted = sorted(res_dict.items(), key=itemgetter(1), reverse=True)
            #     yield lidx, res_tuples_sorted
            # if len(res_dict) == 1:
            #     yield lidx, list(res_dict.items())


    def fmatch(self,
                lkp_table: object,
                on: list[tuple],
                into: str,
                # ret_matches: bool = False
                ):
        """ Perform fuzzy matching between two tables.
            lkp_table: the lookup table to match with.
            on: a list of tuples, each tuple contains (left column, right column, min_ratio).
                eg. [('last_name', 'surname', 80), ('address', 'address', 70)]
            into: the name of the output table to hold the results.
        """
        lkp_table_obj = None
        if isinstance(lkp_table,Memory_Table):
            lkp_table_obj = lkp_table
            lkp_table = lkp_table.name
        else:
            lkp_table_obj = self.bing[lkp_table]
        
        # validate input eg. column etc
        lkeys = [x[0] for x in on] # generate lkeys from on (1st sequence)
        rkeys = [x[1] for x in on] # generate rkeys from on (2nd sequence)
        min_ratios = [x[2] for x in on] # generate ratios from on (3rd sequence)
        lkeys = self.validate_columns(lkeys)
        rkeys = lkp_table_obj.validate_columns(rkeys)
        lcolumn_prematch = self.get_columns() # will use this list when matching occurs below
        # create a table to hold the matches
        matches_tobj = self.bing.create_table(into) # create a table to hold the matches
        # for each matching, update it
        for lidx, res in self._scanfuzzy(lkeys, lkp_table_obj, rkeys, min_ratios):
            print('lidx:',lidx,'res:',res)
            for rank, match_row in enumerate(res, start = 1):
                matches_tobj.insert([lidx, match_row[0], match_row[1], rank], ['lidx', 'ridx', 'ratio', 'rank'])
        return        


    def flookup(self, 
                lkp_table: object, 
                on: list[tuple], 
                ret_columns: list[str] | list[tuple],
                ret_matches: bool = False
                ):      
        lkp_table_obj = None
        if isinstance(lkp_table,Memory_Table):
            lkp_table_obj = lkp_table
            lkp_table = lkp_table.name
        else:
            lkp_table_obj = self.bing[lkp_table]
        
        # validate input eg. column etc
        lkeys = [x[0] for x in on] # generate lkeys from on (1st sequence)
        rkeys = [x[1] for x in on] # generate rkeys from on (2nd sequence)
        min_ratios = [x[2] for x in on] # generate ratios from on (3rd sequence)
        lkeys = self.validate_columns(lkeys)
        rkeys = lkp_table_obj.validate_columns(rkeys)
        lcolumn_prematch = self.get_columns() # will use this list when matching occurs below
        # validate ret_columns once:
        # otherwise will make lots of call later
        valid_ret_columns = []
        for item in ret_columns:
            if isinstance(item,str):
                item = lkp_table_obj.validate_column(item)
                valid_ret_columns.append(item)
            if isinstance(item, tuple):
                valid_column  = lkp_table_obj.validate_column(item[0])
                item_ = tuple([valid_column,item[1]])
                valid_ret_columns.append(item_)
        # add these valid_ret_column to the left table
        for col in valid_ret_columns:
            if isinstance(col, str):
                self.add_column(col)
            if isinstance(col, tuple):
                self.add_column(col[1]) # use the alias
        
        # create a table to hold the matches
        # matches_tobj = self.bing.create_table(into) # create a table to hold the matches
        # for each matching, update it
        for lidx, res in self._scanfuzzy(lkeys, lkp_table_obj, rkeys, min_ratios):
            print('lidx:',lidx,'res:',res)
            # if ret_matches:
            #     for rank, match_row in enumerate(res, start = 1):
            #         matches_tobj.insert([lidx, match_row[0], match_row[1], rank], ['lidx', 'ridx', 'ratio', 'rank'])
            # update this table lrow for each ret_columns
            for item in valid_ret_columns:
                if isinstance(item, str): # if item is a column
                    value = lkp_table_obj[res[0][0]][item]
                    if item in lcolumn_prematch:
                        #print('item',item,'in',self.name)
                        self.update_row(lidx, lkp_table_obj.name + '_' + item, value)
                    else:
                        self.update_row(lidx, item, value)
                if isinstance(item, tuple): # if a tuple (0=column to return from lkp_table, 1=as_column)
                    value = lkp_table_obj[res[0][0]][item[0]]
                    self.update_row(lidx, item[1], value)                        
    

    def groupby(self, 
                    columns, 
                    drop_none: bool = True, 
                    group_count: bool = False, 
                    counts: list[str] | list[tuple] = None,
                    sums: list[str] | list[tuple] = None,
                    mins: list[str] | list[tuple] = None,
                    maxs: list[str] | list[tuple] = None,
                    means: list[str] | list[tuple] = None,
                    group_concat = None) -> None:
        """ Generate a groupby records.
            return Table object."""
        group_tobj = Memory_Table('grouped') # table to hold the result
        group_count_column = 'group_count' if group_count == True else group_count
        # loop the table
        valid_cols = [self.validate_column(col) for col in columns] # validate these columns
        for idx, row in self.iterrows():
            index_records = [row[col] for col in valid_cols]
            if drop_none:
                if index_records.count(None) == len(index_records):
                    continue
            index = tuple([bintang.core._normalize_caseless(col) if isinstance(col, str) else col for col in index_records])
            if not group_tobj.index_exists(index):
                # add record for the first time
                group_tobj.insert(index_records, columns=valid_cols, index=index)
                if group_count:
                    group_tobj.update_row(index, group_count_column, 1)
                if group_concat:
                    if group_concat == True:
                        group_tobj.update_row(index, 'group_concat',[idx])  
                    else:
                        group_tobj.update_row(index, 'group_concat',[row[self.validate_column(group_concat)]])        
                if counts:
                    self._groupby_new_index_count(index, row, counts, group_tobj) 
                if sums:
                    self._groupby_new_index_sum(index, row, sums, group_tobj)
                if mins:
                    self._groupby_new_index_min(index, row, mins, group_tobj)
                if maxs:
                    self._groupby_new_index_max(index, row, maxs, group_tobj)
                if means: # call existing sum and count function
                    temp_sum_columns = self._groupby_new_index_sum(index, row, means, group_tobj, means=True)
                    temp_count_columns = self._groupby_new_index_count(index, row, means, group_tobj, means=True)    
            else:
                if group_count:
                    incremented = group_tobj[index][group_count_column] + 1
                    group_tobj.update_row(index, group_count_column, incremented)
                if group_concat:
                    if group_concat == True:
                        group_tobj.update_row(index, 'group_concat', group_tobj[index]['group_concat'] + [idx])
                    else:
                        group_tobj.update_row(index, 'group_concat', group_tobj[index]['group_concat'] + [row[self.validate_column(group_concat)]])        
                if counts:
                    self._groupby_existing_index_count(index, row, counts, group_tobj)
                if sums:
                    self._groupby_existing_index_sum(index, row, sums, group_tobj)
                if mins:
                    self._groupby_existing_index_min(index, row, mins, group_tobj)
                if mins:
                    self._groupby_existing_index_max(index, row, maxs, group_tobj)
                if means: # call sum and count functions
                    self._groupby_existing_index_sum(index, row, means, group_tobj, means=True)
                    self._groupby_existing_index_count(index, row, means, group_tobj, means=True)
        # loop through the groupped table)
        if means:
            for idx, row in group_tobj.iterrows():
                print(idx, row)
                for i, col in enumerate(means):
                    print('i',i, col)
                    if isinstance(col, str):
                        col_mean = 'mean_' + col
                        group_tobj.update_row(idx, col_mean, row[temp_sum_columns[i]]/row[temp_count_columns[i]])
                    if isinstance(col, tuple):
                        col_mean = col[1]
                        group_tobj.update_row(idx, col_mean, row[temp_sum_columns[i]]/row[temp_count_columns[i]])
            # drop temp columns
            for x in range (len(temp_sum_columns)):
                group_tobj.drop_column(temp_sum_columns[x])
                group_tobj.drop_column(temp_count_columns[x])
        return group_tobj            



  
    def _groupby_new_index_count(self, index, row, counts, group_tobj, means=False):
        count_columns = []
        for col in counts:
            if isinstance(col, str):
                col_cnt = 'count_' + col
                if row[col] is not None:
                    group_tobj.update_row(index, col_cnt, 1)
                else:
                    group_tobj.update_row(index, col_cnt, 0)
                count_columns.append(col_cnt)    
            if isinstance(col, tuple):
                col_cnt = 'mean_count_' + col[1] if means else col[1]
                if row[col[0]] is not None:
                    group_tobj.update_row(index, col_cnt, 1)
                else:
                    group_tobj.update_row(index, col_cnt, 0)
                count_columns.append(col_cnt)
        return count_columns        


    def _groupby_existing_index_count(self, index, row, counts, group_tobj, means=False):
        for col in counts:
            if isinstance(col, str):
                col_cnt = 'count_' + col
                if row[col] is not None:
                    incremented = group_tobj[index][col_cnt] + 1
                    group_tobj.update_row(index, col_cnt, incremented)  
            if isinstance(col, tuple):
                col_cnt = 'mean_count_' + col[1] if means else col[1]
                if row[col[0]] is not None:
                    incremented = group_tobj[index][col_cnt] + 1
                    group_tobj.update_row(index, col_cnt, incremented)

                

    def _groupby_new_index_sum(self, index, row, sums, group_tobj, means=False):
        sum_columns = []
        for col in sums:
            if isinstance(col, str):
                col_sum = 'sum_' + col
                if isinstance(row[col], (int,float)):
                    group_tobj.update_row(index, col_sum, row[col])
                else:
                    group_tobj.update_row(index, col_sum, 0)
                sum_columns.append(col_sum)    
            if isinstance(col, tuple):
                col_sum = 'mean_sum_' + col[1] if means else col[1]
                if isinstance(row[col[0]], (int,float)):
                    group_tobj.update_row(index, col_sum, row[col[0]])
                else:
                    group_tobj.update_row(index, col_sum, 0)
                sum_columns.append(col_sum)
        return sum_columns


    def _groupby_existing_index_sum(self, index, row, sums, group_tobj, means=False):
        for col in sums:
            if isinstance(col, str):
                col_sum = 'sum_' + col
                if isinstance(row[col], (int,float)):
                    summed = group_tobj[index][col_sum] + row[col]
                    group_tobj.update_row(index, col_sum, summed)
            if isinstance(col, tuple):
                col_sum = 'mean_sum_' + col[1] if means else col[1]
                if isinstance(row[col[0]], (int,float)):
                    summed = group_tobj[index][col_sum] + row[col[0]]
                    group_tobj.update_row(index, col_sum, summed)


    def _groupby_new_index_min(self, index, row, mins, group_tobj):
        for col in mins:
            if isinstance(col, str):
                col_min = 'min_' + col
                if isinstance(row[col], (int,float)):
                    group_tobj.update_row(index, col_min, row[col])
                else:
                    group_tobj.update_row(index, col_min, 0)
            if isinstance(col, tuple):
                col_min = col[1]
                if isinstance(row[col[0]], (int,float)):
                    group_tobj.update_row(index, col_min, row[col[0]])
                else:
                    group_tobj.update_row(index, col_min, 0)


    def _groupby_existing_index_min(self, index, row, mins, group_tobj):
        for col in mins:
            if isinstance(col, str):
                col_min = 'min_' + col
                if isinstance(row[col], (int,float)):
                    minned = min(group_tobj[index][col_min], row[col])
                    group_tobj.update_row(index, col_min, minned)
            if isinstance(col, tuple):
                col_sum = col[1]
                if isinstance(row[col[0]], (int,float)):
                    minned = min(group_tobj[index][col_min], row[col[0]])
                    group_tobj.update_row(index, col_min, minned)


    def _groupby_new_index_max(self, index, row, maxs, group_tobj):
        for col in maxs:
            if isinstance(col, str):
                col_max = 'max_' + col
                if isinstance(row[col], (int,float)):
                    group_tobj.update_row(index, col_max, row[col])
                else:
                    group_tobj.update_row(index, col_max, 0)
            if isinstance(col, tuple):
                col_max = col[1]
                if isinstance(row[col[0]], (int,float)):
                    group_tobj.update_row(index, col_max, row[col[0]])
                else:
                    group_tobj.update_row(index, col_max, 0)


    def _groupby_existing_index_max(self, index, row, maxs, group_tobj):
        for col in maxs:
            if isinstance(col, str):
                col_max = 'max_' + col
                if isinstance(row[col], (int,float)):
                    maxed = max(group_tobj[index][col_max], row[col])
                    group_tobj.update_row(index, col_max, maxed)
            if isinstance(col, tuple):
                col_max = col[1]
                if isinstance(row[col[0]], (int,float)):
                    maxed = max(group_tobj[index][col_max], row[col[0]])
                    group_tobj.update_row(index, col_max, maxed)


    def read_csv(self, path, delimiter=',', quotechar='"', header_row=1):
        import csv
        with open(path,newline='') as f:
            reader = csv.reader(f, delimiter=delimiter, quotechar=quotechar)
            # determine columns
            columns = []
            for rownum, row in enumerate(reader, start=1):
                if rownum == header_row:
                    columns = [col for col in row] # add all columns
                    f.seek(0) # return to BOF
                    break 
            
            # insert records
            for rownum, row in enumerate(reader, start=1):
                if rownum > header_row: # assume records after header
                    if len(columns) == len(row):
                        self.insert(row, columns)
                    else:
                        raise IndexError ('length of column and row is not the same at rownum {}. Possible issues were incorrect quoting or missing value.'.format(rownum))


    def iterrows_ws(self, ws, wb_type, header_row=1):
        if wb_type == 'openpyxl':
            for rownum, row_cells in enumerate(ws.iter_rows(min_row=header_row), start=1):
                yield rownum, row_cells
        else: # assume 'xlrd'
            rownum = 1
            for i, row_cells in enumerate(ws, start=1):
                if i < header_row: # simulate 'skiprow' like openpyxl
                    continue # skip
                else:
                    yield rownum, row_cells
                    rownum += 1


    def read_excel(self, wb, sheetname, header_row=1):
        wb_type = bintang.get_wb_type_toread(wb)
        
        # validate sheetname
        if wb_type == 'openpyxl':
            sheetnames_lced = {x.lower(): x  for x in wb.sheetnames}
        else: # assume xlrd
            sheetnames_lced = {x.lower(): x  for x in wb.sheet_names()}

        if sheetname.lower() not in sheetnames_lced:
            similar_sheetnames = bintang.get_similar_values(sheetname, sheetnames_lced)
            raise ValueError ('could not find sheetname {}. Did you mean {}?'.format(repr(sheetname),' or '.join(similar_sheetnames)))
        ws = wb[sheetnames_lced[sheetname.lower()]] # assign with the correct name (caseless) through validated user input.
        columns = []
        Nonecolumn_cnt = 0
        #for rownum, row_cells in enumerate(ws.iter_rows(min_row=header_row),start=1):
        for rownum, row_cells in self.iterrows_ws(ws, wb_type, header_row):
            values = [] # hold column value for each row
            if rownum == 1:
                for cell in row_cells:
                    if cell.value is None:
                        columname = 'noname' + str(Nonecolumn_cnt)
                        Nonecolumn_cnt += 1
                        columns.append(columname)
                    else:
                        columns.append(str(cell.value))
                if Nonecolumn_cnt > 0:
                    log.warning('Warning! Noname column detected!')          
            if rownum > 1:
                for cell in row_cells:
                    values.append(cell.value)
                # if rownum == 370:
                #     log.debug(f'{values} at rownum 370.')
                #     log.debug(any(values))
                if any(values):
                    self.insert(values, columns)


    def read_sql(self, conn, sql_str=None, params=None ):
        cursor = conn.cursor()
        if sql_str is None:
            sql_str = "SELECT * FROM {}".format(self.name)
        if params is not None:
            cursor.execute(sql_str, params)
        else:
            cursor.execute(sql_str)
        columns = [col[0] for col in cursor.description]
        for col in columns:
            self.add_column(col)
        # for row in cursor.fetchall():
        #     self.insert(row, columns) 
        while True:
            rows = cursor.fetchmany(300)
            if not rows: break
            for row in rows:
                self.insert(row, columns)                  


    def reindex(self):
        # dict is already indexed with python 3.7+
        # this only means we rebuild row id so any deleted idx can be reused
        idxs = list(self.__rows)
        for i, idx in enumerate(idxs):
            self.__rows[i] = self.__rows[idx] # reassign
            if i != idx:
                del self.__rows[idx]


    def set_index(self, column):
        grouped = self.groupby([column], group_count=True, group_concat=True)
        temp_test_dup = grouped.filter(lambda row: row['group_count']>1)
        if len(temp_test_dup) > 0:
            # temp_test_dup.name = 'duplicates'
            # temp_test_dup.print()
            dups = Memory_Table('duplicates')
            for idx, row in temp_test_dup.iterrows():
                for _idx in row['group_concat']:
                    dups.insert(self.get_row_asdict(_idx))
            dups.print()
            raise ValueError(f'Duplicates found in column "{column}".')
            #raise ValueError('column must be a single column string!')  
            for idx, row in temp_test_dup.iterrows():
                print(idx, row)
        
        # reset idx using existing column
        temp_tobj = copy.deepcopy(self)
        temp_idxs = list(temp_tobj.__rows)
        # reset table methods
        self.__rows.clear()     # reset all rows
        self.__columns.clear()  # reset all columns
        self.__last_assigned_columnid= 9 #
        self.__last_assigned_rowid = -1 # for use when row created
        self.__last_assigned_idx = -1 # for use when add
        # populate records
        for i in temp_idxs:
            self.insert(temp_tobj.get_row_asdict(i), index=temp_tobj.get_row_asdict(i)[column])
            temp_tobj.delete_row(i)
                    


    def to_csv(self, path, columns=None, index=False, \
               dialect='excel', delimiter=',', \
               quotechar='"', quoting=0): 
        import csv
        # csv.QUOTE_MINIMAL = 0
        # csv.QUOTE_ALL = 1
        # csv.QUOTE_NONNUMERIC = 2
        # csv.QUOTE_NONE = 3
        
        if columns is None:
            columns = self.get_columns()
        with open(path, 'w', newline = '\n') as csvfile:
            csvwriter = csv.writer(csvfile, dialect=dialect, delimiter=delimiter,
                                   quotechar=quotechar, quoting=quoting)
            columns_towrite = [col for col in columns]
            if index:                       # if column index wanted
                idx_col = self.INDEX_COLUMN_NAME
                if isinstance(index, str):  # if user wanted own index column name
                    idx_col = index
                columns_towrite.insert(0,idx_col)
            # write header to csvfile
            csvwriter.writerow(columns_towrite)
            if index:
                for idx, row in self.iterrows(columns, row_type='list'):
                    csvwriter.writerow([idx] + row)
            else:
                for idx, row in self.iterrows(columns, row_type='list'):
                    csvwriter.writerow(row)
            

    def to_dict(self, columns=None):
        if columns is None:
            columns = self.get_columns()
        res = {}
        res['table'] = self.name
        res['columns'] = columns
        res['rows'] = []
        # add row
        for idx, row in self.iterrows(columns):
            res['rows'].append(row)
        return res    
            

    def to_excel(self, wb, path, columns=None, index=False, sheet_title=None):
        wb_type = bintang.get_wb_type_towrite(wb)
        sheet_title = self.name if sheet_title is None else sheet_title
        if wb_type == 'openpyxl':
            ws = wb.active
            ws.title = sheet_title
        else: # assume xlwt as user want to save as xls
            ws = wb.add_sheet(sheet_title)

        # add header
        columns_towrite = []
        if columns is None:
            columns_towrite = self.get_columns()
        else:
            columns_towrite = [col for col in columns]
        # log.debug('index: {}'.format(index))
        if index:
            if index:                          # if column index wanted
                idx_column = self.INDEX_COLUMN_NAME
                if isinstance(index, str):     # if user wanted own index column name
                    idx_column = index      
                columns_towrite.insert(0,idx_column)
            if index != True:
                columns_towrite.insert(0,str(index))        
        # add headers' row
        if wb_type == 'openpyxl':
            ws.append(columns_towrite)
        else: # assume xlwt
            for i, col in enumerate(columns_towrite):
                ws.write(0, i, col)
        # add data row
        for idx, row in self.iterrows(columns, row_type='list'):
            if index:
                row.insert(0,idx)
            if wb_type == 'openpyxl':
                ws.append(row)
            else: # assume xlwt
                for cidx, value in enumerate(row): # enumerate column value
                    ws.write(idx, cidx, value)     # write value for each column

        # if index:
        #     for idx, row in self.iterrows(columns, row_type='list'):
        #         row.insert(0,idx)
        #         ws.append(row)
        # if not index:
        #     for idx, row in self.iterrows(columns, row_type='list'):
        #         ws.append(row)          
        wb.save(path)


    def to_list(self, column):
        if isinstance(column, str):
            res_list = []
            for idx, row in self.iterrows([column]):
                res_list.append(next(iter(row.values())))
            return res_list    
        else:
            raise ValueError('column must be a single column string!')    


    def _to_json_file(self, path, columns=None):
        """ this will be useful for big table
        and we are going to write row by row into a file
        instead a one-oof write"""
        if columns is None:
            columns = self.get_columns()
        with open(path, 'w') as f:
            f.write('{') # write opening json obj
            f.write('{}'.format(json.dumps(self.name)))
            f.write(':')
            f.write('[') # write opening array

            for idx, row in self.iterrows(columns=columns):
                print(json.dumps(row))
                f.write(json.dumps(row))
                if not (idx + 1) == len(self): 
                    f.write(',') # if not eof then write obj seperator
            f.write(']') # write closing array
            f.write('}') # write closing json obj


    def to_json(self):
        """This is just a placeholder.
        Converting dict to Json is trivia by using Json dumps/dump.
        No need a function to write here?"""
        rows_ = []
        for idx, row in self.iterrows():
            rows_.append(row)
        obj_dict  = {}
        obj_dict[self.name] = rows_
        return json.dumps(obj_dict)



class Path_Table(Memory_Table):
    def __init__(self, name, bing=None):
        super().__init__(name, bing=bing)
        self.path = name
        self.children = [] 
               

    def __repr__(self):
        tbl = {}
        tbl['name'] = self.name
        tbl['path'] = self.path
        columns = []
        for k,v in self._Table__columns.items():
            columns.append(dict(id=v.id, name=v.name))
        tbl['columns'] = columns
        return json.dumps(tbl, indent=2)


    def get_path_aslist(self):
        path_as_list = []
        if self.path == '/': # if root
            path_as_list.append('/')
            return path_as_list
        for node in self.path.split("/"):
            if node == '': # if the 1st one. 1st node have discarded by split
                path_as_list.append('/')
            else:
                path_as_list.append(node)
        return path_as_list


    def upsert_table_path_row(self, tprow):
        #log.debug("\n  ------------------in upsert_table_path_row (table.py) --------------------")
        # extract the rowid and use it as the table index (the key of rows{})
        # create a row if the rowid not found in the table's index
        res_idx = self.get_rowidx_byrowid(tprow.id)
        
        if res_idx is None:
            #log.debug(f'inserting... row does not exist {tprow}')
            row = self.make_row(tprow.id)
            # re-make cells from tprow
            for id, c in tprow.cells.items():
                #log.debug(f'{id} cell: {c}')
                if c.is_key == True:
                    cell = self.make_cell(self.PARENT_PREFIX + c.get_column(), c.value)
                else:
                    cell = self.make_cell(c.get_column(), c.value)

                row.add_cell(cell)
            # add to rows
            self.add_row(row)
            
        elif res_idx is not None:
            #log.debug(f"updating... row exists {tprow}")
            for id, c in tprow.cells.items():
                if c.is_key == True:
                    cell = self.make_cell(self.PARENT_PREFIX + c.get_column(), c.value)
                else:
                    cell = self.make_cell(c.get_column(), c.value)
                # self.__rows[res_idx].add_cell(cell)  
                self._Memory_Table__rows[res_idx].add_cell(cell)        
        #log.debug("\n  ------------------out upsert_table_path_row (table.py)-------------------")        
