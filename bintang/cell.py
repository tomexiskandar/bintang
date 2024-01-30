import copy

class Cell(object):
    """Define a cell object
    """
    __slot__ = ('columnid', 'value')
    def __init__(self,columnid,value = None):
        self.columnid = columnid
        self.value = value

    def __repr__(self):
        return "{}(columnid:{}, value: {})".format(__class__.__name__, str(self.columnid), str(self.value))
    

class Cell_Path_List(Cell):
    def __init__(self,columnid, path_list, value):
        super().__init__(columnid, value)
        self.path_list = path_list
        self.is_key = False

    # DEPRECATED - use super() instead
    # def __init__(self,columnid, path_list, value):
    #     Cell.__init__(self, columnid, value)
    #     self.path_list = path_list
            

    def __repr__(self):
        return "{}(columnid:{}, value:{}, path_list:{}, get_column():{}, is_parent_key:{})".format\
            (__class__.__name__, str(self.columnid), str(self.value)\
            ,str(self.path_list), self.get_column(), self.is_key)    
        
    def gen_path_list_norowid(self):
        pathl_norowid = [x for x in self.path_list if not isinstance(x, int)]
        return pathl_norowid
     

    def get_column(self):
        pathl_norowid = self.gen_path_list_norowid()
        colname = pathl_norowid[-1]
        if pathl_norowid.count(colname) > 1: # resolve name if table and column would use the same name
            return colname + '_' + colname
        else:
            return pathl_norowid[-1]