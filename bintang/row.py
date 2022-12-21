from contextlib import contextmanager

class Row:
    """Define a row as a collection of cells
    """ 
    def __init__(self,id):
        self.id = id
        self.cells = {}


    def __repr__(self):
        return '{}(id:{}, cells:{})'.format(__class__.__name__, self.id, self.cells)


    def add_cell(self,cell):
        self.cells[cell.columnid] = cell # add a cell into the cells dictionary and use columnid as the key


    def get_cell(self,columnid):
        cell = self.cells[columnid]
        #print("test direct...",cell.get_cell())
        return cell


    def get_value(self,columnid):
        return self.cells[columnid].value


    def get_values(self,columnids):
        row_values = []
        for columnid in columnids:
            cell_value = None
            if columnid in self.cells:
                cell_value = self.cells[columnid].value
            row_values.append(cell_value)
        return row_values


class Row_Table_Path(Row):
    def __init__(self,id):
        super().__init__(id)
        self.tablepath = None

    # DEPRECATED use super() instead
    # def __init__(self, id):
    #     Row.__init__(self, id)
    #     self.tablepath = None    

    def __repr__(self):
        return '{}(tablepath:{} id:{}, cells:{})'.format(__class__.__name__, self.tablepath, self.id, self.cells)            