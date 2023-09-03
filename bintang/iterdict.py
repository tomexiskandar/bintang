import copy
from bintang.cell import Cell_Path_List
from bintang.row import Row_Table_Path
from bintang.log import log

ROOT_STR = '/'
TABLEPATH_SEP = '/'

def gen_tablepath(path_list):
    pathl_norowid = [x for x in path_list if not isinstance(x, int)]
    if path_list[-1] == pathl_norowid[-1]:
        # this should work for pattern #1 and #2
        del pathl_norowid[-1]
        tablepath = TABLEPATH_SEP.join(pathl_norowid)
        if tablepath =='':
            tablepath = ROOT_STR

        # if len(tablepath) > 2:
        #     tablepath = tablepath[1:]
        #log.debug(tablepath)
        return tablepath
    if (path_list[-2] == pathl_norowid[-1]) and isinstance(path_list[-1], int):
        # this shold work for pattern #3
        #log.debug(pathl_norowid)
        tablepath = TABLEPATH_SEP.join(pathl_norowid)
        # if len(tablepath) > 2:
        #     tablepath = tablepath[1:]
        #log.debug(tablepath)
        
        return tablepath


def gen_rowid(path_list):
    if isinstance(path_list[-1], str):
        pathl_copy = copy.deepcopy(path_list)
        del pathl_copy[-1]
        return ''.join(str(x) for x in pathl_copy)
    if isinstance(path_list[-1], int):
        return ''.join(str(x) for x in path_list)
    

def gen_table_path_row(path_list, value):
    # there are two types of cell will be created as the result of data path location
    # 1. main cell
    # 2. parent cell (if any)
    # for the path_list 
    rowid = gen_rowid(path_list)
    row = Row_Table_Path(rowid)
    row.tablepath = gen_tablepath(path_list)
    pathl = copy.deepcopy(path_list) 
    
    log.debug('\n---------------------in gen_table_path_row (travdict.py)--------------------------')
    log.debug(f'  {pathl}-> path_list {path_list}')
    
    # # create the first cell    
    # cell = Cell_Path_List(len(pathl), path_list, value)
    # row.add_cell(cell)
    # if debug:
    #     print('  {}-> {}'.format(len(pathl),cell))
    

    # take the last item of the path
    if isinstance(pathl[-1], int):
        # if a list
        log.debug('  a list...')
        del pathl[-1]
        del pathl[-1]
        log.debug(f'  {len(pathl)}-> pathl after {pathl}')
    elif isinstance(pathl[-1], str):
        # if a dict
        log.debug('  a dict...')
        del pathl[-1]
    
    # create 'parent key' cells
    while len(pathl) > 1: # if 0 then the root (/) will be included in the result set
        log.debug(f'  {len(pathl)}->>> pathl {pathl}')
        #create the cell
        if isinstance(pathl[-1], int):
            # this is a list
            pathl_copy = copy.deepcopy(pathl) # to make it persistent and be used in making cell    
            cell = Cell_Path_List(len(pathl), pathl_copy, pathl[-1])
            cell.is_key = True
            row.add_cell(cell)
            log.debug(f' len pathl {len(pathl)}-> cell {cell}')
            del pathl[-1]
            del pathl[-1]
            
        elif isinstance(pathl[-1], str):
            # this is a dict
            pathl_copy = copy.deepcopy(pathl) # to make it persistent and be used in making cell    
            cell = Cell_Path_List(len(pathl), pathl_copy, pathl[-1])
            cell.is_key = True
            row.add_cell(cell)
            log.debug(f'  len pathl {len(pathl)}-> cell {cell}')
            del pathl[-1]

    # create the cell
    pathl_norowid = [x for x in path_list if not isinstance(x, int)]
    cell = Cell_Path_List(len(pathl), path_list, value)
    row.add_cell(cell)
    log.debug(f'  len pathl {pathl}-> cell {cell}')
    log.debug('---------------------out gen_table_path_row (travdict.py)-----------------------') 
    return row           
    

def _iterdict(dict_or_list, path=['']):
    if isinstance(dict_or_list, dict):
        iterator = dict_or_list.items()
    else:
        iterator = enumerate(dict_or_list)
    for k, v in iterator:
        yield path + [k], v
        if isinstance(v, (dict, list)):
            for k, v in _iterdict(v, path + [k]):
                yield k,v


def iterdict(dict_obj, tablepaths=None):
    # yield a tprow (row with a table path)
    # also allow to yield only the row that matches tablepath list arg.
    for path_list, value in _iterdict(dict_obj): 
        #log.debug(f'{i} k-> {path_list} v--> {value}')
        if isinstance(value,(list,dict)):
            continue # use continue and comment the below line value = None to create column with list/dict
            #value = '[]'  # include column with list/dict in the table but set value as None
        # print(path_list, value)
        if tablepaths is None: #len(tablepaths) == 0:
            # client wants to return all available tablepaths
            yield gen_table_path_row(path_list,value)
        else: #elif len(tablepaths) > 0:
            # client wants to return specific tablepaths
            for tp in tablepaths:
                if tp == gen_tablepath(path_list):
                    yield gen_table_path_row(path_list,value)