# bintang
A tiny and temporary db for quick data cleansing and transformation.

# Requirements
1. Python 3.6 or higher
2. openpyxl

# Installation
pip install bintang

# Examples of Usage
```
>>> import bintang  #  import the package   
>>> bt = bintang.Bintang() # bintang object created  
>>> bt.create_table('Person')  
>>> bt  
{  
  "name": null,  
  "tables": [  
      "Person"  
  ]  
}  
```
Use insert function to insert data. The args is a pair of column names and values and must be a list or tuple.
```
>>> bt['Person'].insert(['id','name','age','address'], [1,'John',35,'1 Station St'])  
>>> bt['Person'].insert(['id','name','age','hobby','address'], [2,'Jane',17,'Reading','8 Parade Rd'])  
>>> bt['Person'].insert(['id','name','hobby','address'], [3,'Nutmeg','Fishing','7 Ocean Rd'])  
>>> bt['Person'].insert(['id','name','hobby','address'], [4,'Maria','Digging',None])  
>>> bt['Person'].insert(['id','name','hobby'], [5,'Bing','Digging'])
```
Loop your data using iterows function. This will loop through all the rows one by one in a python dict.
```
>>> for idx, row in bt['Person'].iterrows():  
...     print(idx, row)  
...  
0 {'id': 1, 'name': 'John', 'age': 35, 'address': '1 Station St', 'hobby': None}  
1 {'id': 2, 'name': 'Jane', 'age': 17, 'address': '8 Parade Rd', 'hobby': 'Reading'}  
2 {'id': 3, 'name': 'Nutmeg', 'age': None, 'address': '7 Ocean Rd', 'hobby': 'Fishing'}  
3 {'id': 4, 'name': 'Maria', 'age': None, 'address': None, 'hobby': 'Digging'}  
4 {'id': 5, 'name': 'Bing', 'age': None, 'address': None, 'hobby': 'Digging'} 
```
You should notice that all column names now have been normalised for each row, even though all records have not used all column names during insertion.
 
Inspect Person table column list. You can also use function table.get_columnnames() to list all column names.
```
>>> bt['Person']  
{  
  "table name": "Person",  
  "columns": [  
    {  
      "id": 0,  
      "name": "id"  
    },  
    {  
      "id": 1,  
      "name": "name"  
    },  
    {  
      "id": 2,  
      "name": "age"  
    },  
    etc...
```
Use update function to change the data. The function signature is table.update(columnname, value, where=None). The value and where arg can use lambda function for smarter expression.
```
>>> bt['Person'].update('age', 10, where=lambda row: row['name']=='Maria') 
```
Use getitem function to retrieve the row stored at the 3rd index
``` 
>>> bt['Person'][3] 
{'id': 4, 'name': 'Maria', 'age': 10, 'address': None, 'hobby': 'Digging'} 
```

We are updating [the wiki pages] (https://github.com/tomexiskandar/bintang/wiki) so that's where you can find more about Bintang. 
