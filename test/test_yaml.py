import yaml
from bintang import Bintang

if __name__ == '__main__':
    with open ('zetup_data_example.yaml') as stream:
        setup_data = yaml.safe_load(stream)

    # print(setup_data)
    # print(type(setup_data))
    # items = setup_data['setup']
    # for k,v in items.items():
    #     print(k,v)

    bt = Bintang()
    bt.read_dict(setup_data)
    print(bt)
    #bt['/setup/lines'].print_columns_info()
    
    bt.print()
    #bt['/setup/lines'].print_columns_info()