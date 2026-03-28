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

    p = bt['Person']  # set p as the table bt['Person'] alias so the codes more readable
    p.insert(['id','name','surname','Address'], [3,'Okie','Dokie','7 Ocean Rd'])
    p.insert(['id','name','hobby','Address'], [4,'Maria','Digging','7 Heaven Ave'])
    p.insert(['id','name','hobby','Address'], [5,'Bing','Digging',None])

    expected_rows = [
        {'id': 1, 'name': 'John', 'surname': 'Smith', 'address': '1 Station St', 'hobby': None},
        {'id': 2, 'name': 'Jane', 'surname': 'Brown', 'address': '8 Parade Rd', 'hobby': 'Digging'},
        {'id': 3, 'name': 'Okie', 'surname': 'Dokie', 'address': '7 Ocean Rd', 'hobby': None},
        {'id': 4, 'name': 'Maria', 'surname': None, 'address': '7 Heaven Ave', 'hobby': 'Digging'},
        {'id': 5, 'name': 'Bing', 'surname': None, 'address': None, 'hobby': 'Digging'}
    ]

    assert len(p) == len(expected_rows)

    for idx, row in p.iterrows():
        assert row == expected_rows[idx -1] # decreased idx to match expected_rows index list.

    p.update('age', 10, where=lambda row: row['name']=='Maria')
    assert 'age' in p.get_columns()
    assert p.get_row_asdict(4)['age'] == 10

    p.rename_column('name', 'first_name')
    assert 'name' not in  p.get_columns() # 'name must not be in the columns list
    assert 'first_name' in p.get_columns() # 'first_name must be in the column list

    assert p.get_value('first_name', where=lambda row: row['id']==1) == 'John'
