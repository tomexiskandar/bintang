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
        self.columns = {}


    def __len__(self):
        """ return the length of rows"""
        with open(self.filepath, newline='') as f:
            reader = csv.reader(f, delimiter=self.delimiter, quotechar=self.quotechar)
            row_count = len(list(reader)) -1
        return row_count


    def get_columnid(self, column):
        return next((k for k, v in self.columns.items() if v == column), None)


    def get_columnids(self, columns):
        pass

    def populate_columns(self):
        columns = self.get_columns()
        for i, col in enumerate(columns, start=1):
            self.columns[i] = col    


    def get_columns(self):
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
                 columns: list | tuple | None = None, 
                 row_type: str='dict', 
                 where=None, 
                 rowid: str=None):

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
                for rownum, csvrow in enumerate(reader, start=1):
                    if len(self.columns) == len(csvrow):
                        row = self.make_row()
                        rowid_value = None
                        for k, v in self.columns.items():
                            cell = Cell(k, csvrow[k-1])
                            row.add_cell(cell)
                            if rowid == v:
                                rowid_value = csvrow[k-1]
                        yield rowid_value, row
                    else:
                        raise IndexError ('length of column and row is not the same at rownum {}. Possible issues were incorrect quoting or missing value.'.format(rownum))
            else:
                for rownum, csvrow in enumerate(reader, start=1):
                    if len(self.columns) == len(csvrow):
                        row = self.make_row()
                        for k, v in self.columns.items():
                            cell = Cell(k, csvrow[k-1])
                            row.add_cell(cell)
                        yield rownum, row
                    else:
                        raise IndexError ('length of column and row is not the same at rownum {}. Possible issues were incorrect quoting or missing value.'.format(rownum))

    

    
    def iterrows(self, 
                 columns: list | tuple | None = None, 
                 row_type: str='dict', 
                 where=None, 
                 rowid: str=None):

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
                    if len(self.columns) == len(row):
                        row_dict = dict(zip(self.columns.values(), row))
                        # print('row_dict before where:', row_dict)
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
                    if len(self.columns) == len(row):
                        row_dict = dict(zip(self.columns.values(), row))
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
            