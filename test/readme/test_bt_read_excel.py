import bintang
from bintang import Bintang



if __name__ == '__main__':

    bt = Bintang()
    #bt.create_table('Person') 
    path = r"C:\Users\60145210\Documents\Projects\bintang\test\contacts.xlsx"
    bt.read_excel(path, ['Person','Group'])

    print(bt)
    bt['Person'].print()
    # quit()

    # bt.create_table('Person1')
    # bt['Person1'].read_excel(path, 'Person', header_row = 3)

    # bt['Person1'].print()
    # #bt['Person1'].add_column('suriname')

    #bt['Person1'].check_column('surnamex')
    #bt['Person1'].validate_column('surnamex')
    #bt.drop_table('Person2')
    # test error tab = bt['Person2']
    quit()