from bintang import Bintang
from pathlib import Path
TEST_DIR = Path(__file__).resolve().parents[1] / 'tests'

def test_bintang_table_valrows_csv():
    bt = Bintang("test_bt_valrows")
    bt.create_csv_linked_table('product', TEST_DIR / 'product.csv')
    assert len(bt['product']) > 0

    # update column properties
    import datetime
    p = bt['product']
    p.update_column('id', data_type='int', min_value=1, required=True)
    p.update_column('name', data_type='str', min_length=0, max_length=20, empty_string_as_null=True)
    p.update_column('price', data_type='float', min_value=0)
    p.update_column('order_date', data_type='date', max_value=datetime.date(2025,12,31), empty_string_as_null=True)
    p.update_column('timestamp', data_type='datetime', empty_string_as_null=False)

    valid_rows = []
    bad_rows = []
    for idx, results in p.valrows(empty_string_as_null=True):
        if all([x[2] for x in results]): # check if all columns are valid:
            # do something with valid rows, for example, print a message
            print(f'valid row: idx:{idx} results: {results}')
            valid_rows.append(idx)
        else:
            print(f'bad row: idx:{idx} results: {results}')
            bad_rows.append(idx)

    assert valid_rows == [1, 3]
    assert bad_rows == [2, 4, 5, 6]
