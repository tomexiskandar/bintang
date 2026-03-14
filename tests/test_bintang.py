from bintang import Bintang
import pytest
import datetime

@pytest.fixture(scope="session")
def bt():
    """ fixture for bintang object
    """
    bt = Bintang("test_bt")
    return bt

def test_bt_init(bt):
    assert bt.name == "test_bt"
    assert isinstance(bt, Bintang)

def test_create_table(bt):
    bt.create_table("Person")
    assert "Person" in bt.get_tables()

def test_insert_person_table(bt):
    """ test inserting rows into Person table
        the insert method should accept both dict and list/tuple
    """

    bt['Person'].insert({'id': 1, 'name': 'John', 'surname': 'Smith', 'address': '1 Station St'})

    expected_columns = ('id', 'name', 'surname', 'address')
    assert bt['Person'].get_columns() == expected_columns

    # insert another record with new column, hobby.
    bt['Person'].insert(['id','name','surname','hobby','address'], [2,'Jane','Brown','Digging','8 Parade Rd'])

    expected_columns = ('id', 'name', 'surname', 'address', 'hobby')
    assert bt['Person'].get_columns() == expected_columns # hobby column should be added

    bt['Person'].insert(['id','name','surname','Address'], [3,'Okie','Dokie','7 Ocean Rd'])
    bt['Person'].insert(['id','name','hobby','Address'], [4,'Maria','Digging','7 Heaven Ave'])
    bt['Person'].insert(['id','name','hobby','Address'], [5,'Bing','Digging',None])

    expected_rows = [
        {'id': 1, 'name': 'John', 'surname': 'Smith', 'address': '1 Station St', 'hobby': None},
        {'id': 2, 'name': 'Jane', 'surname': 'Brown', 'address': '8 Parade Rd', 'hobby': 'Digging'},
        {'id': 3, 'name': 'Okie', 'surname': 'Dokie', 'address': '7 Ocean Rd', 'hobby': None},
        {'id': 4, 'name': 'Maria', 'surname': None, 'address': '7 Heaven Ave', 'hobby': 'Digging'},
        {'id': 5, 'name': 'Bing', 'surname': None, 'address': None, 'hobby': 'Digging'}
    ]

    assert len(bt['Person']) == len(expected_rows)

    for idx, row in bt['Person'].iterrows():
        assert row == expected_rows[idx -1] # decreased idx to match expected_rows index list.

    bt['Person'].update('age', 10, where=lambda row: row['name']=='Maria')
    assert 'age' in bt['Person'].get_columns()
    assert bt['Person'].get_row_asdict(4)['age'] == 10


def void_test_valrows(bt):
    try:
        import dateutil
    except ImportError as e:
        print(f"Warning: {e}. Required modulde not found. Exiting...")
        return None
        
    # create product table
    bt.create_table('Product')
    p = bt['Product']
    p.add_column('id', data_type='int', min_value=1, required=True)
    p.add_column('name', data_type='str', min_length=1, max_length=20)
    p.add_column('price', data_type='float', min_value=0)
    p.add_column('order_date', data_type='date', max_value=datetime.date(2025,12,31))

    # insert valid row
    p.insert({'id':1, 'name':'Hook','price':'1.60', 'order_date':'2025-07-01'})

    # insert invalid row with None or empty string id
    p.insert({'id':'', 'name':'Sinker','price':1.20})

    # insert valid row
    p.insert({'id':'3', 'name':'Reels  ','price':15.50}) 

    # insert invalid row with order date above the maximum value
    p.insert({'id':4, 'name':None, 'price':20, 'order_date': '2026-07-01'})

    # insert invalid row with non-convertable id
    p.insert({'id':'5t', 'name':'Bait','price':20})

    # insert invalide row with length of name above the maximum length
    p.insert({'id':6, 'name':'Bait has very long name','price':5.50})

    
    p.validate('Product_Bad_Data')

    assert len(p) == 2  # expect only two rows are valid
    assert (1,3) == p.get_all_indexes()   # expect the valid row is 1 and 3


    assert (len(bt['Product_Bad_Data'])) == 4   # expect four rows are invalid
    assert (2,4,5,6) ==  bt['Product_Bad_Data'].get_all_indexes()  # expect the invalid rows are 2,4,5,6
    