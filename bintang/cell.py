import copy
class Cell:
    """Define a cell object
    """
    def __init__(self,columnid,value = None):
        self.columnid = columnid
        self.value = value

    def __repr__(self):
        return "{}(columnid:{}, value: {})".format(__class__.__name__, str(self.columnid), str(self.value))
    

class Cell_JSON(Cell):
    def __init__(self,columnid, path_list, value):
        super().__init__(self)
        self.columnid = columnid
        self.path_list = path_list
        self.value = value

    def __repr__(self):
        return "{}(columnid:{}, value:{}, path_list:{}, get_columnname():{})".format(__class__.__name__, str(self.columnid), str(self.value)\
            ,str(self.path_list), self.get_columnname())    
        
    def gen_path_list_norowid(self):
        pathl_norowid = [x for x in self.path_list if not isinstance(x, int)]
        return pathl_norowid
     

    def get_columnname(self):
        pathl_norowid = self.gen_path_list_norowid()
        return pathl_norowid[-1]
        
            
        



  
