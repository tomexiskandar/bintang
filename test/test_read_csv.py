import csv
import bintang

print(csv.list_dialects())
if __name__ == '__main__':
    path = r"C:\Users\60145210\Documents\Projects\bintang\test\data\Person.csv"
    # path = r"C:\Users\60145210\Documents\Projects\bintang\test\data\csv_test.csv"
    with open(path, newline='') as csvfile:
        csvreader = csv.reader(csvfile, delimiter=',', quotechar='"')
        #csvreader = csv.reader(csvfile)
        # for row in csvreader:
        #     pass #print(row)
        # row = next(csvreader)
        # print(row)
        # row = next(csvreader)
        # print(row)

    bt = bintang.Bintang()
    bt.create_table('Person')
    bt['Person'].read_csv(path ) 
    bt['Person'].print()
    bt.print()