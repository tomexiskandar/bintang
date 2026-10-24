import time
from collections import Counter
from pathlib import Path
import bintang
from bintang.row import Row

if __name__ == '__main__':
    this_path = Path(__file__).parent
    fpath = this_path / "local_person.csv"

    bt = bintang.Bintang()
    
    start_time = time.time() # Get current time in seconds since the epoch

    bt.create_csv_linked_table('person', fpath)
    expected_columns = ['id', 'name', 'surname', 'address','hobby','score']
    assert Counter(expected_columns) == Counter(bt['person'].get_columns())
    assert 5 == len(bt['person'])
    for idx, row in bt['person'].iterrows(row_type='list'):
        assert 1 == idx
        assert isinstance(row, list)
        break
    for idx, row in bt['person'].iterrows(row_type='dict'):
        assert 1 == idx
        assert isinstance(row, dict)
        break
    colid = bt['person'].get_columnid('name')
    for idx, row in bt['person']._iterrows():
        assert 1 == idx
        assert isinstance(row, Row)
        assert 'John' == row.cells[colid].value
        break
    
