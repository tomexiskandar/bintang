from bintang.log import log
from bintang.table_base import Base_Table
from bintang.cell import Cell
import csv

class From_CSV_Table(Base_Table):
    def __init__(self, name, filepath, bing=None, delimiter=',', quotechar='"', header_row=1):
        super().__init__(name, bing=bing)
        self.filepath = filepath
        self.delimiter = delimiter
        self.quotechar = quotechar
        self.header_row = header_row
        # self.columns = {}


    def __len__(self):
        """ return the length of rows"""
        with open(self.filepath, newline='') as f:
            reader = csv.reader(f, delimiter=self.delimiter, quotechar=self.quotechar)
            row_count = len(list(reader)) -1
        return row_count


    def populate_columns(self):
        columns = self.get_columns_from_file()
        for column in columns: 
            columnid = self.get_columnid(column)
            if columnid is None: # if columnid is None then assume user wants a new column
                self.add_column(column)



    def get_columns_from_file(self):
        with open(self.filepath, newline='') as f:
            reader = csv.reader(f, delimiter=self.delimiter, quotechar=self.quotechar)
            # determine columns
            columns = []
            for rownum, row in enumerate(reader, start=1):
                if rownum == self.header_row:
                    columns = [col for col in row] # add all columns
                    f.seek(0) # return to BOF
            return tuple(columns)
                    

    def _iterrows(self, 
                 where=None, 
                 rowid: str=None):

        columns_dict = {} # to hold column id to column name mapping
        for column in self.get_columns():
            cobj = self.get_column_object(column)
            columns_dict[cobj.id] = column
        with open(self.filepath, newline='') as f:
            reader = csv.reader(f, delimiter=self.delimiter, quotechar=self.quotechar)
            next(reader)
            # iterate each line
            # for i in range(self.header_row):
            #     print('i', i)
            #     next(reader)
            # to simplify this logic, split it into two, whether rowid provided or not.
            if rowid is not None:
                print('rowid:', rowid)
                for rownum, csvrow in enumerate(reader, start=1):
                    row_dict = dict(zip(columns_dict.keys(), csvrow))
                    if len(columns_dict) == len(csvrow):
                        row = self.make_row()
                        try:
                            rowid_value = row_dict[self.get_columnid(rowid)]
                        except KeyError:
                            raise KeyError(f"Row ID column '{rowid}' not found in CSV row.")
                        for k, v in row_dict.items():
                            cell = Cell(k, v)
                            row.add_cell(cell)
                        yield rowid_value, row
                    else:
                        raise IndexError ('length of column and row is not the same at rownum {}. Possible issues were incorrect quoting or missing value.'.format(rownum))
            else:
                for rownum, csvrow in enumerate(reader, start=1):
                    row_dict = dict(zip(columns_dict.keys(), csvrow))
                    if len(columns_dict) == len(csvrow):
                        row = self.make_row()
                        for k, v in row_dict.items():
                            cell = Cell(k, v)
                            row.add_cell(cell)
                        yield rownum, row
                    else:
                        raise IndexError ('length of column and row is not the same at rownum {}. Possible issues were incorrect quoting or missing value.'.format(rownum))

    

    
    def iterrows(self, 
                 columns: list | tuple | None = None, 
                 row_type: str='dict', 
                 where=None, 
                 rowid: str=None):
        if columns is None:
            columns = self.get_columns()
        else:
            columns = self.validate_columns(columns)
        import csv
        with open(self.filepath, newline='') as f:
            reader = csv.reader(f, delimiter=self.delimiter, quotechar=self.quotechar)
            next(reader)
            # iterate each line
            # for i in range(self.header_row):
            #     print('i', i)
            #     next(reader)
            # split this logic into two, whether rowid provided or not to simplify the logics 
            if rowid is not None:
                for rownum, row in enumerate(reader, start=1):
                    if len(columns) == len(row):
                        row_dict = dict(zip(columns, row))
                        if columns is not None:
                            # print('columns arg', columns)
                            row_dict = {col: row_dict[col] for col in columns}
                            if row_type == 'list':
                                yield row_dict[rowid], [row_dict[col] for col in columns]
                            else:
                                yield row_dict[rowid], row_dict
                        else:
                            if row_type == 'list':
                                yield row_dict[rowid], row
                            else:
                                yield row_dict[rowid], row_dict
                    else:
                        raise IndexError ('length of column and row is not the same at rownum {}. Possible issues were incorrect quoting or missing value.'.format(rownum))
            else:
                for rownum, row in enumerate(reader, start=1):
                    # assert len(self.get_columns()) == len(columns)
                    if len(columns) == len(row):
                        row_dict = dict(zip(columns, row))
                        # print('row_dict before where:', row_dict)
                        if columns is not None:
                            # print('columns arg', columns)
                            row_dict = {col: row_dict[col] for col in columns}
                            if row_type == 'list':
                                yield rownum, [row_dict[col] for col in columns]
                            else:
                                yield rownum, row_dict
                        else:
                            if row_type == 'list':
                                yield rownum, row
                            else:
                                yield rownum, row_dict
                    else:
                        raise IndexError ('length of column and row is not the same at rownum {}. Possible issues were incorrect quoting or missing value.'.format(rownum))



    def set_to_sql_colmap(self, columns):
        if isinstance(columns, list) or isinstance(columns, tuple):
            return dict(zip(columns, columns))
        elif isinstance(columns, dict):
            return columns
            