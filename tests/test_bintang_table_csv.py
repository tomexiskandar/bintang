from bintang import Bintang
from pathlib import Path
TEST_DIR = Path(__file__).resolve().parents[1] / 'tests'

def test_bintang_table_csv():
    bt = Bintang("test_bt")
    bt.create_table("person")
    bt['person'].insert({'id':1,'name':'John','surname':'Smith','address':'1 Station St'})
    bt['person'].insert(['id','name','surname','hobby','address'],[2,'Jane','Brown','Digging','8 Parade Rd'])
    bt['person'].insert(['id','name','surname','Address'], [3,'Okie','Dokie', '7 Ocean Rd'])
    bt['person'].insert(('id','name','hobby','Address'),(4,'Maria','Digging','7 Heaven Ave'))
    bt['person'].insert(('id','name','hobby','Address'),(5,'Bing','Digging',None))

    bt.create_table("fishing_club")
    bt['fishing_club'].insert({'FirstName':'Ajes','LastName':'Freeman','Membership':'Active'})
    bt['fishing_club'].insert({'FirstName':'John','LastName':'Smith','Membership':'Active'})
    bt['fishing_club'].insert({'FirstName':'Jane','LastName':'Brown','Membership':'Active'})
    bt['fishing_club'].insert({'FirstName':'Nutmeg','LastName':'Spaniel','Membership':'Active'})
    bt['fishing_club'].insert({'FirstName':'Zekey','LastName':'Pokey','Membership':'Active'})

    # Create CSV files for testing
    csv_path_person = TEST_DIR / 'person.csv'
    csv_path_person.unlink(missing_ok=True)
    bt['person'].to_csv(csv_path_person)
    assert csv_path_person.exists()

    csv_path_fishing_club = TEST_DIR / 'fishing_club.csv'
    csv_path_fishing_club.unlink(missing_ok=True)
    bt['fishing_club'].to_csv(csv_path_fishing_club)
    assert csv_path_fishing_club.exists()

    # test reading back the CSV files using bintang table
    bt.create_table("person_from_csv")
    bt['person_from_csv'].read_csv(csv_path_person)
    assert len(bt['person_from_csv']) == len(bt['person'])

    bt.create_table("fishing_club_from_csv")
    bt['fishing_club_from_csv'].read_csv(csv_path_fishing_club)
    assert len(bt['fishing_club_from_csv']) == len(bt['fishing_club'])

    # test reading back the CSV files using standard csv module
    import csv
    with open(csv_path_person, newline='') as f:
        reader = csv.DictReader(f)
        rows = list(reader)
        assert len(rows) == len(bt['person'])

    with open(csv_path_fishing_club, newline='') as f:
        reader = csv.DictReader(f)
        rows = list(reader)
        assert len(rows) == len(bt['fishing_club'])

    # test reading back the CSV files using csv linked table
    bt.create_csv_linked_table('person_from_csv_linked',csv_path_person)
    assert len(bt['person_from_csv_linked']) == len(bt['person'])

    bt.create_csv_linked_table('fishing_club_from_csv_linked', csv_path_fishing_club)
    assert len(bt['fishing_club_from_csv_linked']) == len(bt['fishing_club'])