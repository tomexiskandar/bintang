from bintang.cell import Cell_JSON
from bintang.row import Row_JSON
import copy



def traverse_(dict_or_list, path=[]):
    if isinstance(dict_or_list, dict):
        iterator = dict_or_list.items()
    else:
        iterator = enumerate(dict_or_list)
    for k, v in iterator:
        yield path + [k], v
        if isinstance(v, (dict, list)):
            for k, v in traverse_(v, path + [k]):
                yield k,v


def gen_tablepath(path_list):
    pathl_norowid = [x for x in path_list if not isinstance(x, int)]
    if path_list[-1] == pathl_norowid[-1]:
        # this should work for pattern #1 and #2
        del pathl_norowid[-1]
        tablepath = '/'.join(pathl_norowid)
        if len(tablepath) > 2:
            tablepath = tablepath[1:]
        return tablepath
    if (path_list[-2] == pathl_norowid[-1]) and isinstance(path_list[-1], int):
        # this shold work for pattern #3
        tablepath = '/'.join(pathl_norowid)
        if len(tablepath) > 2:
            tablepath = tablepath[1:]
        return tablepath


def gen_rowid(path_list):
    if isinstance(path_list[-1], str):
        pathl_copy = copy.deepcopy(path_list)
        del pathl_copy[-1]
        return ''.join(str(x) for x in pathl_copy)
    if isinstance(path_list[-1], int):
        return ''.join(str(x) for x in path_list)
    

def gen_cell_json_row(path_list, value):
    # there are two types of cell will be created as the result of data path location
    # 1. main cell
    # 2. parent cell (if any)
    # for the path_list 
    debug = False
    rowid = gen_rowid(path_list)
    row = Row_JSON(rowid)
    row.tablepath = gen_tablepath(path_list)
    pathl = copy.deepcopy(path_list) 
    
    if debug:
        print('\n---------------------in gen_cell_json_row (travjson.py)--------------------------')
        print('  {}-> path_list {}'.format(len(pathl),path_list))
    
    # create the first cell    
    cell = Cell_JSON(len(pathl), path_list, value)
    row.add_cell(cell)
    if debug:
        print('  {}-> {}'.format(len(pathl),cell))
    

    # take the last item of the path
    if isinstance(pathl[-1], int):
        # if a list
        if debug: print('  a list...')
        del pathl[-1]
        del pathl[-1]
        if debug:
            print('  {}-> pathl after {}'.format(len(pathl),pathl))
    elif isinstance(pathl[-1], str):
        # if a dict
        if debug: print('  a dict...')
        del pathl[-1]
    
    
    # create 'parent key' cells
    while len(pathl) > 1: # if 0 then the root (/) will be included in the result set
        if debug:
            print('  {}-> pathl {}'.format(len(pathl),pathl))
        #create the cell
        if isinstance(pathl[-1], int):
            # this is a list
            pathl_copy = copy.deepcopy(pathl) # to make it persistent and be used in making cell    
            cell = Cell_JSON(len(pathl), pathl_copy, pathl[-1])
            row.add_cell(cell)
            if debug:
                print('  {}-> {}'.format(len(pathl),cell))
            del pathl[-1]
            del pathl[-1]
            
        elif isinstance(pathl[-1], str):
            # this is a dict
            pathl_copy = copy.deepcopy(pathl) # to make it persistent and be used in making cell    
            cell = Cell_JSON(len(pathl), pathl_copy, pathl[-1])
            row.add_cell(cell)
            if debug:
                print('  {}-> {}'.format(len(pathl),cell))
            del pathl[-1]
    if debug:
        print('---------------------out gen_cell_json_row (travjson.py)-----------------------')
    return row           
    


def traverse_json(jsondata,tablepaths=[]):
    # yield a json row
    # also allow to yield only the row that matches tablepath list from client.
    for path_list, value, in traverse_(jsondata,path=['/']):        
        if isinstance(value,(list,dict)):
            #print('-->',path_list,":",value)
            #continue # use continue and comment the below line value = None to create column with list/dict
            value = None  # include column with list/dict in the table but set value as None
        # print(path_list, value)
        if len(tablepaths) == 0:
            # client wants to return all available tablepaths
            yield gen_cell_json_row(path_list,value)
        elif len(tablepaths) > 0:
            # client wants to return specific tablepaths
            for tp in tablepaths:
                if tp == gen_tablepath(path_list):
                    yield gen_cell_json_row(path_list,value)