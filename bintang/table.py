import json
# import sqlite3
import uuid
import re
import sys
import copy
from operator import itemgetter
from pathlib import Path
from typing import Callable
import warnings
import inspect
from abc import ABC, abstractmethod
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
        self.bing = bing
        self.name = name
        self.INDEX_COLUMN_NAME = 'idx'
        self.PARENT_PREFIX = ''
        self.type_map = type_map # assign type_map to self.type_map

    
    def get_schema_name(self) -> str | None:
        splitname = self.name.split('.')
        if len(splitname)==2:
            return splitname[0]

    
    def get_table_name(self) -> str | None:
        splitname = self.name.split('.')
        if len(splitname)==2:
            return splitname[1]
        elif len(splitname) == 1:
            return self.name    


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



