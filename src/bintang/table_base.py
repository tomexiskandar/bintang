import json
# import sqlite3
import uuid
import re
import sys
import copy
from pathlib import Path
from typing import Callable
import warnings
import inspect
from itertools import product
from abc import ABC, abstractmethod
from bintang.log import log
import bintang
import types
from bintang.row import Row

MAX_ROW_SQL_INSERT = 300

def custom_formatwarning(msg, *args, **kwargs):
    # ignore everything except the message
    return str(msg)

warnings.formatwarning = custom_formatwarning


class ColumnError(Exception):
    def __init__(self,message):
        self.message = message
    


class Base_Table(ABC):
    """
    Base class for all table types.
    This class provides the basic structure and methods for table manipulation. 
    It is not intended to be instantiated directly, but rather to be subclassed.
    """
    def __init__(self,name, bing=None):
        self.bing: bintang.Bintang = bing # reference to parent Bintang object
        self.name: str = name
        self.INDEX_COLUMN_NAME: str = 'idx'
        self.PARENT_PREFIX: str = ''
        self.__last_assigned_rowid = 0
        self.type_map: dict = type_map # assign type_map to self.type_map
        #self.pytype: dict = pytype # assign pytype to self.pytype

    
    @abstractmethod
    def get_columns(self):
        pass

    @abstractmethod
    def get_columnid(self):
        pass

    @abstractmethod
    def get_columnids(self):
        pass

    @abstractmethod
    def iterrows(self):
        pass

    def ColumnError(self):
        """Raise ColumnError with table name and column name."""
        return ColumnError(self.name, self.column)
    

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

    
    def _get_columnnames_lced(self, columns=None) -> dict:
        return {col.lower(): col  for col in self.get_columns()}


    def validate_columns(self, columns):
        """return columns from those stored in table.columns"""
        validated_cols = []
        unmatched_cols = []
        for column in columns:
            if column.lower() in self._get_columnnames_lced().keys():
                validated_cols.append(self._get_columnnames_lced().get(column.lower()))
            else:
                unmatched_cols.append(column)        
        if len(unmatched_cols) > 0:
            similar_columns = self._suggest_similar_columns(unmatched_cols)
            message = self._suggest_columns_msg(similar_columns)
            raise ColumnError(message)
        else:
            return validated_cols 


    def _suggest_similar_columns(self, columns, min_ratio=0.7):
        res = {}
        for col in columns:
            similar_cols = bintang.get_similar_values(col, self.get_columns())
            res[col] = ['{}'.format(x) for x in similar_cols]
        return res  


    def _suggest_columns_msg(self, suggested_columns):   
        unmatched_cols = [x for x in suggested_columns.keys()]
        if len(suggested_columns) > 0:
            message = f"No such {', '.join(unmatched_cols)} column{'(s)' if len(suggested_columns)>1 else ''} in {self.name} table."
            line_msg = []
            for col, suggestion  in suggested_columns.items():
                msg = f" For {col}, did you mean: {', '.join(suggestion)}?"
                line_msg.append(msg)
            # construct message
            for msg in line_msg:
                message += msg
            return message                 

    
    def get_schema_name(self) -> str | None:
        """ Returns the schema of the table.
        Schema is the second part from the right of the table name.
        For example, if the table name is 'public.Person', it returns 'public'.
        """
        parts = self.name.split('.')
        if len(parts) > 1: 
            return parts[-2]


    def extract_schema_name(self, fqtn: str) -> str | None:
        """
        Extracts the schema from a fully qualified table name (fqtn).
        Schema name is the second part from the right of the fqtn.
        For example, if fqtn is 'public.Person', it returns 'public'.
        """
        parts = fqtn.split('.')
        if len(parts) > 1: 
            return parts[-2]
             

    def get_table_name(self) -> str | None:
        parts = self.name.split('.')
        return parts[-1]


    def extract_table_name(self, fqtn) -> str | None:
        parts = fqtn.split('.')
        return parts[-1]


    def _get_sql_conn_name_xp(self, conn):
        # experimenting to get connector name
        # so code can act differently for eg. params list used in prepared statement.
        if str(type(conn)) == "<class 'sqlite3.Connection'>":
            return 'sqlite3'
        elif str(type(conn)) == "<class 'pyodbc.Connection'>":
            return 'pyodbc'
        elif str(type(conn)) == "<class 'psycopg.Connection'>":
            return 'psycopg'
        elif str(type(conn)) == "<class 'psycopg2.extensions.connection'>":
            return 'psycopg2'
        else:
            raise ValueError('Sorry Only sqlite3, pyodbc and psycopg connection accepted!')

            
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


    def cmprows(self, lkp_table: str, on: list[tuple] = None, min_matches=1, find_all=True):
        print('len of on', on)
        """
        compare rows in the current table with rows in the lkp_table.
        Args:
            lkp_table (str): the name of the table to compare with aka right table, the index called 'ridx'.
            on (list[str]): a list of tuples where each tuple contains two column names to compare.
                            If None, it will compare all columns in both tables.
            min_matches (int): number of matches to complete when on is None. Default is 1
            find_all (bool): if True (default) then check all the lookup data, while  If False then exit on first match
        Yields:
            lidx, [(ridx,(lcol1,rcol1), ((lcol1,rcol1))]
            1 [(2, ((11, 11), (12, 12)))]
        """
        if on:
            req_matches = len(on)
        else:
            lcolumns = self.get_columns()
            rcolumns = self.bing[lkp_table].get_columns()
            on = list(product(lcolumns,rcolumns))
            req_matches = min_matches 
        on_ = [] # will hold column id keys instead column name
        for tup in on:
            tup_ = (self.get_columnid(tup[0]), self.bing[lkp_table].get_columnid(tup[1]))
            on_.append(tup_) 
        for lidx, lrow in self._iterrows():
            results = []
            for ridx, rrow in self.bing[lkp_table]._iterrows():
                matches = 0
                matched_columns = []
                col_results = []
                for i in range(len(on_)):
                    if on_[i][0] in lrow.cells and on_[i][1] in rrow.cells: # check if both cells exist
                        if bintang.match(lrow.cells[on_[i][0]].value
                                        ,rrow.cells[on_[i][1]].value
                                        ):
                            matches += 1
                            matched_columns.append((on_[i][0] 
                                                ,on_[i][1]
                                                ))
                            cmp = (on_[i][0], on_[i][1])                    
                            col_results.append(cmp)
                if matches >= req_matches:
                    results.append((ridx,tuple(col_results)))
                    #yield lidx, ridx, tuple(matched_columns)
                    if not find_all:
                        break  # only return the first match
            if results: # yield only when the results is True/not empty
                yield lidx, results    
    
    
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
    
    
    def to_sql(self, conn: object, 
               table: str, 
               columns: list[str]=None,
               method: str='prepared', 
               max_rows: int = 1) -> int:
        """conn: accept only pyodbc.Connection or psycopg.Connection
           table: table name in the database and can use a fully qualified table name eg. schema_name.table_name.
           columns: If a dictionary then a columns mapping where the key is sql column (destination) and the value is bintang columns (source). 
                    If a list, column mapping will be created automatically assuming source columns and destination columns are the same. 
                    If not provided it assumes that user wants to insert all the columns from the table.
           method: prepared (default) or string
           return-> rows_affected: number of rows affected by the insert operation.
        """
        conn_name = self._get_sql_conn_name_xp(conn)
        if columns is None: # check if user want to include all columns in the table
            columns = self.get_columns()
        else:
            if isinstance(columns,list) or isinstance(columns, tuple):
                columns= self.validate_columns(columns)
            elif isinstance(columns,dict):
                columns_key = [x for x in columns.keys()]
                columns_val = [x for x in columns.values()] ## trouble maker
                columns_val= self.validate_columns(columns_val) ## trouble maker
                columns = dict(zip(columns_key, columns_val))
            else: 
                raise ValueError('Error! Only list or dict allowed for columns.')

        schema = self.extract_schema_name(table)
        table = self.extract_table_name(table)

        if method.upper() =='STRING':
            return self._to_sql_string(conn, table, columns, schema=schema, max_rows=max_rows, conn_name=conn_name)
        else:
            return self._to_sql_prep(conn, table, columns, schema=schema, max_rows=max_rows,conn_name=conn_name)

 
    def _to_sql_string(self, conn, table, columns, schema=None, max_rows = 300, conn_name='pyodbc'):
        colmap = self.set_to_sql_colmap(columns)
        src_cols = [x for x in colmap.values()]
        dest_columns = [x for x in colmap.keys()]
        if schema is not None:
            sql_template = 'INSERT INTO "{}"."{}" ({}) VALUES'
            str_stmt = sql_template.format(schema,table,",".join(['"{}"'.format(x) for x in colmap]))
        else:
            sql_template = 'INSERT INTO "{}" ({}) VALUES'
            str_stmt = sql_template.format(table,",".join(['"{}"'.format(x) for x in colmap]))
        
        # check if getting type info is supported, if yes then get sql data type and literal
        res = None
        try:
            res = self.get_sql_typeinfo_table(conn)
        except Exception as e:
            log.error(e)
            log.warning('Getting SQL type info is not supported for this connection, will proceed without getting SQL data type and literal. This may cause issue for some data types and may cause all values to be treated as string literal.')
        if res is not None:
            sql_cols_withtype = self.set_sql_datatype(dest_columns, conn, schema, table)
            sql_cols_withliteral = self.set_sql_literal(sql_cols_withtype, conn)
        else:
            sql_cols_withliteral = None

        cursor = conn.cursor()
        temp_rows = []  
        rows_affected = 0 # to hold total record affected
        for idx, values in self.iterrows(src_cols, row_type='list'):
            if sql_cols_withliteral is not None:    
                sql_record = self.gen_sql_record_with_literal(values, sql_cols_withliteral)
            else:   
                sql_record = self.gen_sql_record_without_literal(values)
            temp_rows.append(sql_record)
            if len(temp_rows) == max_rows:
                stmt = str_stmt + ' {}'.format(",".join(temp_rows))
                try:
                    log.debug(stmt)
                    cursor.execute(stmt)
                    rows_affected += cursor.rowcount
                except Exception as e:
                    log.error(e)
                    log.error(stmt)
                temp_rows.clear()
        if len(temp_rows) > 0:
            stmt = str_stmt + ' {}'.format(",".join(temp_rows))
            try:
                log.debug(stmt)
                cursor.execute(stmt)
                rows_affected += cursor.rowcount
            except Exception as e:
                log.error(e)
                log.error(stmt)
        return rows_affected           


    def gen_sql_record_with_literal(self, values, sql_cols_withliteral):
        sql_record = []
        for (col, value) in zip(sql_cols_withliteral.keys(), values):
            literals = sql_cols_withliteral[col]
            sql_value = self.gen_sql_value_with_literal(value, literals)
            sql_record.append(sql_value)
        return "({})".format(','.join(sql_record))


    def gen_sql_record_without_literal(self, values):
        sql_record = []
        for value in values:
            sql_value = self.gen_sql_value_without_literal(value)
            sql_record.append(sql_value)
        return "({})".format(','.join(sql_record))    
        

    def gen_sql_value_with_literal(self, value, literals):
        if value == "" or value is None:
            return "NULL"
        if isinstance(value, str):
            value = value.replace("'","''")
        return f"{'' if literals[0] is None else literals[0]}{value}{'' if literals[1] is None else literals[1]}"


    def gen_sql_value_without_literal(self, value):
        if value == "" or value is None:
            return "NULL"
        if isinstance(value, str):
            value = value.replace("'","''")
        
        if type(value) in [int, float, bool]:
            return str(value) # so we can just join them later
        else:
            return f"'{value}'"


    def get_sql_typeinfo_table(self, conn):
        cursor = conn.cursor()
        sql_type_info_tuple = cursor.getTypeInfo(sqlType = None)
        columns_ = [column[0] for column in cursor.description]
        sql_typeinfo = {}
        for row in sql_type_info_tuple:
            row_dict = dict(zip(columns_, row))
            type_name = row_dict['type_name']     
            sql_typeinfo[type_name] = row_dict
        return sql_typeinfo
    

    def set_sql_datatype(self, dest_columns, conn, schema, table):
        cursor = conn.cursor()
        try:
            sql_columns = cursor.columns(schema=schema, table=table)
            columns_ = [column[0] for column in cursor.description]
            col_dict = {}
            for row in sql_columns: 
                row_dict = dict(zip(columns_, row))
                column_name = row_dict['column_name']
                col_dict[row_dict['column_name']] = row_dict
            sql_columns_withtype = {}    
            for col in dest_columns:
                sql_columns_withtype[col] = col_dict[col]['type_name']    
            return sql_columns_withtype
        except Exception as e: 
            log.error(e)
            return None  
    

    def set_sql_literal(self, sql_cols_withtype, conn):
        try:
            sql_typeinfo_tab = self.get_sql_typeinfo_table(conn)           
            sql_cols_withliteral = {}
            for k, v in sql_cols_withtype.items():
                prefix = sql_typeinfo_tab[v]['literal_prefix']
                suffix = sql_typeinfo_tab[v]['literal_suffix']
                literals = (prefix,suffix)
                sql_cols_withliteral[k] = literals
            return sql_cols_withliteral    
        except Exception as e: # opps... no support
            log.error(e)
            literals = (None, None)
            return {col:literals for col in sql_cols_withtype.keys()}
        


    def _to_sql_prep(self, conn, table, columns, schema=None, max_rows = 1, conn_name='pyodbc'):
        if max_rows <= len(self): # validate max_row
            mrpb = max_rows # assign max row per batch
        else:
            #log.warning('Warning! max_rows {} set greater than totalrows {}. max_rows set to 1'.format(max_rows, len(self)))
            mrpb = 1
        numof_col = len(columns) # num of columns
        colmap = self.set_to_sql_colmap(columns)
        src_cols = [x for x in colmap.values()]
        dest_columns = [x for x in colmap.keys()]
        
        # generate prepared statement
        prep_stmt = self.gen_prep_stmt(table, dest_columns, numof_col, mrpb, conn_name=conn_name, schema=schema)
        
        cursor = conn.cursor()
        temp_rows = []
        rows_affected = 0
        for idx, row in self.iterrows(src_cols, row_type='list'):
            for v in row:
                temp_rows.append(v)
            if len(temp_rows) == (mrpb * numof_col):
                log.debug(prep_stmt)
                log.debug(temp_rows)
                try:
                    cursor.execute(prep_stmt, temp_rows)
                    rows_affected += cursor.rowcount
                except Exception as e:
                    log.error(e)
                    log.error(prep_stmt)
                    log.error(temp_rows)  
                temp_rows.clear()     
        
        if len(temp_rows) > 0: # if any reminder
            # create as prepared statement
            mrpb = int(len(temp_rows)/numof_col) # adjust mrpb
            prep_stmt = self.gen_prep_stmt(table, dest_columns, numof_col, mrpb, conn_name=conn_name, schema=schema)
            try:
                cursor.execute(prep_stmt, temp_rows)
                rows_affected += cursor.rowcount
            except Exception as e:
                log.error(e)
                log.error(prep_stmt)
                log.error(temp_rows)
        return rows_affected


    def gen_prep_stmt(self, table, dest_columns, numof_col, mrpb, conn_name='pyodbc', schema=None):
        param_markers = self.gen_row_param_markers(numof_col, mrpb, conn_name=conn_name)
        if schema is not None:
            sql_template = 'INSERT INTO "{}"."{}" ({}) VALUES {}'
            prep_stmt = sql_template.format(schema, table, ",".join(['"{}"'.format(x) for x in dest_columns]),param_markers)
        else:
            sql_template = 'INSERT INTO "{}" ({}) VALUES {}'
            prep_stmt = sql_template.format(table, ",".join(['"{}"'.format(x) for x in dest_columns]),param_markers)
        return prep_stmt
                

    def gen_row_param_markers(self,numof_col,num_row, conn_name='pyodbc'):
        p = "%s" if conn_name[:7] =='psycopg' else "?" # param marker is different for pyodbc and psycopg, so we need to check the connection name
        param = "(" + ",".join([p]  *numof_col) + ")"
        params = []
        for i in range(num_row):
            params.append(param)
        return ",".join(params)

        
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


    def gen_sql_stmt_merge_dev(self
                           ,trg_table
                           ,dbms = None
                           ,key_columns = None
                           ,proc_name = None
                           ):
        SQU = type_map[dbms]['delimited_identifiers']['start']
        EQU = type_map[dbms]['delimited_identifiers']['end']
        columns = self.get_columns()

        # generate 'on' clause
        on_colmap_str = '<TO BE DEFINED>'
        on_colmap = []
        if key_columns is not None:
            for col in key_columns:
                item = self.quote('src.' + col, dbms) + ' = ' + self.quote('trg.' + col, dbms)       
                on_colmap.append(item)
            on_colmap_str = ' AND '.join(on_colmap)

        # generate colmap for update clause
        update_colmap = []
        for col in columns:
            if col not in key_columns:
                item = self.quote(col, dbms) + ' = ' + self.quote('src', dbms) + '.' + self.quote(col, dbms) + '\n\t\t'
                update_colmap.append(item)
        update_colmap_str = ','.join(update_colmap)

        merge_stmt = f"""
        MERGE INTO {self.quote(trg_table, dbms)} AS trg
        USING {self.quote(self.name, dbms)} AS src
        ON ({on_colmap_str})
        WHEN MATCHED THEN
            UPDATE SET {update_colmap_str}
        WHEN NOT MATCHED BY TARGET THEN
            INSERT ({','.join([self.quote(col, dbms) for col in columns])})
            VALUES ({','.join([self.quote('src', dbms) + '.' + self.quote(col, dbms) for col in columns])})
        WHEN NOT MATCHED BY SOURCE THEN
            DELETE
        ;
        """
        # gen proc_name
        schema_str = ''
        schema = self.get_schema_name()
        if schema is not None:
            schema_str = self.quote(schema, dbms) + '.'

        proc_name_str = self.quote(f'Merge_{self.get_table_name()}_To_TRG', dbms)
        proc_name = f'{schema_str}{proc_name_str}' if proc_name is None else 'proc_name'

        proc_stmt = f"""
        CREATE OR ALTER PROCEDURE {proc_name}
        AS
        BEGIN
        {merge_stmt}
        END
        """
        return proc_stmt


    def quote(self, name, dbms):
        STARTQ = type_map[dbms]['delimited_identifiers']['start']
        ENDQ = type_map[dbms]['delimited_identifiers']['end']
        split_name = name.split('.')
        return '.'.join([STARTQ + x + ENDQ for x in split_name])


    def gen_sql_stmt_truncate_table_dev(self, dbms, proc_name = None):
        schema_str = ''
        schema = self.get_schema_name()
        if schema is not None:
            schema_str = self.quote(schema, dbms) + '.'
        
        truncate_stmt = f"""
        TRUNCATE TABLE {self.quote(self.name, dbms)}
        """
        proc_name_str = self.quote(f'Truncate_Table_{self.get_table_name()}', dbms)
        proc_name = f'{schema_str}{proc_name_str}' if proc_name is not None else 'proc_name'
        proc_stmt = f"""
        CREATE OR ALTER PROCEDURE {proc_name}
        AS
        BEGIN
        {truncate_stmt}
        END
        """
        return proc_stmt         



type_map = {
    'sqlite': {
        'type_mappings': {
            'str':'text'
            ,'int':'int'
            ,'datetime':'datetime'
            ,'float':'real'
            ,'bool':'int'
            ,'bytes':'blob'
        },
        'delimited_identifiers':{'start':'"', 'end':'"'},
        'type_info': {
        'varchar': {'literal_prefix':"'", 'literal_suffix':"'"}
        } 
    },
    'sqlserver': {
        'type_mappings': {
            'str':'nvarchar'
            ,'int':'int'
            ,'datetime':'datetime'
            ,'float':'float'
            ,'bool':'bit'
            ,'bytes':'varbinary'
        },
        'delimited_identifiers':{'start':'[', 'end':']'},
        'type_info': {
        'varchar': {'literal_prefix':"'", 'literal_suffix':"'"}
        } 
    },
    'postgresql': {
        'type_mappings': {
            'str':'varchar'
            ,'int':'INTEGER'
            ,'datetime':'timestamp'
            ,'float':'float8'
            ,'bool':'boolean'
            ,'bytes':'bytes'
        },
        'delimited_identifiers':{'start':'"', 'end':'"'},
        'type_info': {
            'varchar': {'literal_prefix':"'", 'literal_suffix':"'"}
        }   
    }            
}




