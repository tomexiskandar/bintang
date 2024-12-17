import bintang



if __name__ == '__main__':

    bt = bintang.Bintang()
    bt.create_table('Person') 

    bt['Person'].insert([1,'John','Smith','1 Station, St'], ['id','name','surname','address'])
    bt['Person'].insert([2,'Jane','Brown','Digging','8 Parade Rd'], ['id','name','surname','hobby','address'])
    bt['Person'].insert([3,'Okie','Dokie','7 Ocean Rd'], ['id','name','surname','Address'])
    bt['Person'].insert((4,'Maria','Digging','7 Heaven Ave'), ('id','name','hobby','Address'))
    bt['Person'].insert((5,'Bing','Digging',None), ('id','name','hobby','Address'))


    bt['Person'].print()
    bt['Person'].to_csv(r"C:\Users\60145210\Documents\Projects\bintang\test\data\Person.csv", delimiter=',', quotechar='"')
    
