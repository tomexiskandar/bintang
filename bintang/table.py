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
from abc import ABC, abstractmethod
from bintang.log import log
import bintang

MAX_ROW_SQL_INSERT = 300

def custom_formatwarning(msg, *args, **kwargs):
    # ignore everything except the message
    return str(msg)

warnings.formatwarning = custom_formatwarning


class ColumnNotFoundError(Exception):
    def __init__(self,table, column):
        self.message = "Cannot find column '{}' in table {}.".format(column, table)
        super().__init__(self.message)
    


class Base_Table(ABC):
    """
    Base class for all table types.
    This class provides the basic structure and methods for table manipulation. 
    It is not intended to be instantiated directly, but rather to be subclassed.
    """
    def __init__(self,name, bing=None):
        self.bing: bintang.Bintang = bing #
        self.name: str = name
        self.INDEX_COLUMN_NAME: str = 'idx'
        self.PARENT_PREFIX: str = ''
        self.type_map: dict = type_map # assign type_map to self.type_map

    
    @abstractmethod
    def get_columns(self):
        pass


    @abstractmethod
    def iterrows(self):
        pass

    
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
        # print(validated_cols)
        # print(unmatched_cols)
        # quit()        
        if len(unmatched_cols) > 0:
            #raise ColumnNotFoundError(self.name, column)
            res = self._suggest_similar_columns(unmatched_cols)
            res_msg = self._suggest_columns_msg(res)
            raise ValueError(res_msg)
        else:
            return validated_cols

    def validate_columns(self, columns):
        """return columns from those stored in table.columns"""
        validated_cols = []
        unmatched_cols = []
        for column in columns:
            if column.lower() in self._get_columnnames_lced().keys():
                validated_cols.append(self._get_columnnames_lced().get(column.lower()))
            else:
                unmatched_cols.append(column)
        # print(validated_cols)
        # print(unmatched_cols)
        # quit()        
        if len(unmatched_cols) > 0:
            #raise ColumnNotFoundError(self.name, column)
            res = self._suggest_similar_columns(unmatched_cols)
            res_msg = self._suggest_columns_msg(res)
            raise ValueError(res_msg)
        else:
            return validated_cols 


    def _suggest_similar_columns(self, columns, min_ratio=75):
        res = {}
        for col in columns:
            # extracted = process.extract(col, self.get_columns(), scorer=fuzz.ratio, processor=utils.default_process)
            # res[col] = ['{}'.format(x[0]) for x in extracted if x[1] > min_ratio]
            similar_cols = bintang.get_similar_values(col, self.get_columns())
            res[col] = ['{}'.format(x) for x in similar_cols]
        return res  


    def _suggest_columns_msg(self, suggested_columns):   
        unmatched_cols = [x for x in suggested_columns.keys()]
        if len(suggested_columns) > 0:
            message = f'table {self.name} has no column {unmatched_cols}.\n' 
            line_msg = []
            for col, suggestion  in suggested_columns.items():
                msg = f' for column {repr(col)}, did you mean: {suggestion}?\n'
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
        else:
            raise ValueError('Sorry Only sqlite3, pyodbc and psycopg connection accepted!')   


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

 
    def _to_sql_string(self, conn, table, columns, schema=None, max_rows = 300, conn_name='psycopg'):
        colmap = self.set_to_sql_colmap(columns)
        src_cols = [x for x in colmap.values()]
        dest_columns = [x for x in colmap.keys()]
        if schema is not None:
            sql_template = 'INSERT INTO "{}"."{}" ({}) VALUES'
            str_stmt = sql_template.format(schema,table,",".join(['"{}"'.format(x) for x in colmap]))
        else:
            sql_template = 'INSERT INTO "{}" ({}) VALUES'
            str_stmt = sql_template.format(table,",".join(['"{}"'.format(x) for x in colmap]))
        
        sql_cols_withtype = self.set_sql_datatype(dest_columns, conn, schema, table)
        sql_cols_withliteral = None if sql_cols_withtype is None else self.set_sql_literal(sql_cols_withtype, conn)
        cursor = conn.cursor()
        temp_rows = []  
        rows_affected = 0 # to hold total record affected
        for idx, values in self.iterrows(src_cols, row_type='list'):
            sql_record = self.gen_sql_literal_record(values, sql_cols_withliteral)
            temp_rows.append(sql_record)
            if len(temp_rows) == max_rows:
                stmt = str_stmt + ' {}'.format(",".join(temp_rows))
                try:
                    cursor.execute(stmt)
                    rows_affected += cursor.rowcount
                except Exception as e:
                    log.error(e)
                    log.error(stmt)
                temp_rows.clear()
        if len(temp_rows) > 0:
            stmt = str_stmt + ' {}'.format(",".join(temp_rows))
            try:
                cursor.execute(stmt)
                rows_affected += cursor.rowcount
            except Exception as e:
                log.error(e)
                log.error(stmt)
        return rows_affected           


    def gen_sql_literal_record(self, values, sql_cols_withliteral=None):
        sql_record = []
        if sql_cols_withliteral:
            sql_columns = [x for x in sql_cols_withliteral.keys()]
        for i, value in enumerate(values):
            if sql_cols_withliteral:
                col = sql_columns[i]
                literals = sql_cols_withliteral[col]
                sql_value = self.gen_sql_literal_value(value, literals)
            else:
                sql_value = self.gen_sql_literal_value(value)
            sql_record.append(sql_value)
        return "({})".format(','.join(sql_record))
        

    def gen_sql_literal_value(self, value, literals=None):
        if value == "" or value is None:
            return "NULL"
        if isinstance(value, str):
            value = value.replace("'","''")
        if literals:    
            return "{}{}{}".format('' if literals[0] is None else literals[0], value, '' if literals[1] is None else literals[1])
        else:
            if type(value) in [int, float, bool]:
                return str(value) # so we can just join them later
            else:
                return f"'{value}'"
            # return "{}{}{}".format('' if literals[0] is None else literals[0], value, '' if literals[1] is None else literals[1])
    

    def get_sql_typeinfo_table(self, conn):
        cursor = conn.cursor()
        sql_type_info_tuple = cursor.getTypeInfo(sqlType = None)
        columns_ = [column[0] for column in cursor.description]
        tobj = Memory_Table('sql_typeinfo')
        for row in sql_type_info_tuple:
            tobj.insert(row, columns_)
        return tobj
    

    def set_sql_datatype(self, dest_columns, conn, schema, table):
        cursor = conn.cursor()
        try:
            sql_columns = cursor.columns(schema=schema, table=table)
            columns_ = [column[0] for column in cursor.description]
            tobj = Memory_Table('sql_columns_')
            for row in sql_columns: #cursor.columns(schema=schema, table=table):
                tobj.insert(row, columns_)
            sql_columns_withtype = {}    
            for col in dest_columns:
                _type = tobj.get_value('type_name', where = lambda row: row['column_name']==col)
                sql_columns_withtype[col] = _type
            return sql_columns_withtype
        except: # oppss.. no support then fill up None
            return None  
    

    def set_sql_literal(self, sql_cols_withtype, conn):
        try:
            sql_typeinfo_tab = self.get_sql_typeinfo_table(conn)
            sql_cols_withliteral = {}
            for k, v in sql_cols_withtype.items():
                prefix = sql_typeinfo_tab.get_value('literal_prefix',where=lambda row: row['type_name']==v)
                suffix = sql_typeinfo_tab.get_value('literal_suffix',where=lambda row: row['type_name']==v)
                literals = (prefix,suffix)
                sql_cols_withliteral[k] = literals
            return sql_cols_withliteral    
        except: # opps... no support
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
        prep_stmt = self.gen_prep_stmt(table, dest_columns, numof_col, mrpb, schema=schema)
        
        cursor = conn.cursor()
        temp_rows = []
        rows_affected = 0
        for idx, row in self.iterrows(src_cols, row_type='list'):
            for v in row:
                temp_rows.append(v)
            if len(temp_rows) == (mrpb * numof_col):
                # log.debug(prep_stmt)
                # log.debug(temp_rows)
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
            prep_stmt = self.gen_prep_stmt(table, dest_columns, numof_col, mrpb, schema=schema)
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
        p = "%s" if conn_name == 'psycopg' else "?"
        param = "(" + ",".join([p]  *numof_col) + ")"
        params = []
        for i in range(num_row):
            params.append(param)
        return ",".join(params)


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



