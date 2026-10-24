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
            'str':'varchar'
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

from datetime import datetime
from pathlib import Path
import unicodedata

def gen_ts_string(fmt: str = None) -> str:
    """
    Generates a timestamp string. Defaults to YYYYMMDDTHHMMSS 
    (ISO 8601 basic format) if no format is provided.
    """
    now = datetime.now()
    if fmt is None:
        return now.strftime('%Y%m%dT%H%M%S')
    return now.strftime(fmt)


def gen_ts_path(file_path: str, fmt: str = '%Y%m%dT%H%M%S') -> str:
    """
    Appends a timestamp to a filename while preserving the directory path.
    Example: 'c:/data/source.csv' -> 'c:/data/source_20260314T093500.csv'
    """
    original_path = Path(file_path)
    timestamp = datetime.datetime.now().strftime(fmt)
    
    # original_path.parent gives you "c:\data"
    # original_path.stem gives you "incoming_data"
    # original_path.suffix gives you ".csv"
    new_filename = f"{original_path.stem}_{timestamp}{original_path.suffix}"
    
    # joinpath puts the folder and the new filename back together correctly
    return str(original_path.parent.joinpath(new_filename))


def quote_id(name, dbms=None):
    if dbms is None: # default to use SQL Standard quoting
        STARTQ, ENDQ = '"', '"'
    else:
        STARTQ = type_map[dbms]['delimited_identifiers']['start']
        ENDQ = type_map[dbms]['delimited_identifiers']['end']
    split_name = name.split('.')
    return '.'.join([STARTQ + x + ENDQ for x in split_name])    


def default_stringify(value):
    """ convert value to string, normalize it and lower case it.
        This is used to normalize the value before comparing it.
        It aimes to cover default process that SequenceMatcher does not handle.
    """
    if isinstance(value, str):
        value = value.strip()
        value =unicodedata.normalize("NFKD", value.casefold()) #experiment
        return value.lower()
    else:
        return str(value)


# define get_fuzzy_ratio() to use rapidfuzz or difflib depending on package availability
try:
    from rapidfuzz import fuzz , process, utils
    def get_fuzzy_ratio(value1, value2, default_process=True):
        """ get string similarity between two values.
            using rapidfuzz package only
        """
        if value1 is None or value2 is None:
            return 0  
        elif default_process:
            return  round(fuzz.ratio(value1, value2, processor=utils.default_process) /100.0 ,4)
        else:
            return  round(fuzz.ratio(value1, value2) /100.0, 4)
except ImportError as e:
    log.warning(f'warning! bintang.get_fuzzy_ratio() could not find rapidfuzz, difflib.SequenceMatcher would be used.')
    # define get_fuzzy_ratio to use difflib
    from difflib import SequenceMatcher
    def get_fuzzy_ratio(value1, value2, default_process=True):
        """ get string similarity between two values.
            using python difflib's SequenceMatcher only
        """
        if default_process == True:
            value1 = default_stringify(value1)
            value2 = default_stringify(value2)
            return round(SequenceMatcher(None, value1, value2).ratio(), 4)
        else:
            return round(SequenceMatcher(None, value1, value2).ratio(), 4)


def match_case(value1, value2):
    "Ascii case sensitive matching"
    if value1 == value2:
        return True
    

def match_caseless(value1, value2):
    "Ascii ase insensitive matching"
    if isinstance(value1, str) and isinstance(value2, str):
        if value1.lower() == value2.lower():
            return True
    elif isinstance(value1, str) or isinstance(value2, str):
        if str(value1) == str(value2):
            return True    
    else:
        if value1 == value2:
            return True    



def _normalize_caseless(string):
    return unicodedata.normalize("NFKD", string.casefold())


def match(value1, value2):
    """Unicode case insensitive matching.
    the ultimate goal is to get result regardless data type"""
    if isinstance(value1, str) and isinstance(value2, str):
        if _normalize_caseless(value1) == _normalize_caseless(value2):
            return True
    elif isinstance(value1, str) or isinstance(value2, str): # 1 vs '1' is True
        if str(value1) == str(value2):
            return True
    else:
        if value1 == value2:
            return True  

    
def get_similar_values(value, similar_values, min_ratio=0.6):
        res = []
        for col in similar_values:
            ratio = get_fuzzy_ratio(col, value)  
            if ratio >= min_ratio:
                res.append((col,ratio))
        res_sorted = sorted(res, key=lambda tup: tup[1], reverse=True)
        return [x[0] for x in res_sorted] # just extract the name, not ratio


def get_wb_type_toread(wb):
    """
        get workbook type so read_excel() will know which func/attrb to use.
    """
    if str(type(wb)) == "<class 'openpyxl.workbook.workbook.Workbook'>":
        return 'openpyxl'
    elif str(type(wb)) == "<class 'xlrd.book.Book'>":
        return 'xlrd'
    else:
        raise ValueError('Sorry, for reading excel file, only openpyxl or xlrd workbook accepted!')
    

def get_wb_type_towrite(wb):
    """
        get workbook type so to_excel() know which func/attrb to use.
    """
    if str(type(wb)) == "<class 'openpyxl.workbook.workbook.Workbook'>":
        return 'openpyxl'
    elif str(type(wb)) == "<class 'xlwt.Workbook.Workbook'>":
        return 'xlwt'
    else:
        raise ValueError('Sorry, for writing excel file, only openpyxl or xlwt workbook accepted!')