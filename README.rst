=======
Bintang
=======
A tiny and temporary db for quick data cleansing and transformation.
It is a high-level Python coding and would help anyone up to speed with ETL work.

.. contents:: Overview
   :depth: 3

------------------
How to get started
------------------


Requirements
------------
1. Python 3.7 or higher
2. rapidfuzz
3. openpyxl


Installation
------------

.. code-block:: python

   pip install bintang


Examples of Usage
-----------------

.. code-block:: python

   from bintang import Bintang # import the package
   bt = Bintang()              # bintang object created

   bt.create_table('Person')  
   print(bt  )
   #{  
   #   "name": null,  
   #   "tables": [  
   #   "Person"  
   #   ]  
   #}  


Use insert function to populate a table. The parameters are record and columns.

.. code-block:: python

   # Create a couple of tables and use insert function to insert data.
   bt.create_table('Person') 

   bt['Person'].insert([1,'John','Smith','1 Station St'], ['id','name','surname','address'])
   bt['Person'].insert([2,'Jane','Brown','Digging','8 Parade Rd'], ['id','name','surname','hobby','address'])
   bt['Person'].insert([3,'Okie','Dokie','7 Ocean Rd'], ['id','name','surname','Address'])
   bt['Person'].insert((4,'Maria','Digging','7 Heaven Ave'), ('id','name','hobby','Address'))
   bt['Person'].insert((5,'Bing','Digging',None), ('id','name','hobby','Address'))

   bt.create_table("FishingClub")
   bt['FishingClub'].insert(['Ajes','Freeman','Active'], ['FirstName','LastName','Membership'])
   bt['FishingClub'].insert(['John','Smith','Active'], ['FirstName','LastName','Membership'])
   bt['FishingClub'].insert(['Jane','Brown','Active'], ['FirstName','LastName','Membership'])
   bt['FishingClub'].insert(['Nutmeg','Spaniel','Active'], ['FirstName','LastName','Membership'])
   bt['FishingClub'].insert(['Zekey','Pokey','Active'], ['FirstName','LastName','Membership'])

Loop your data using iterrows function. This will loop through all the rows one by one in a Python dict.

.. code-block:: python

   for idx, row in bt['Person'].iterrows():
       print(idx, row)  
  
   # 0 {'id': 1, 'name': 'John', 'surname': 'Smith', 'address': '1 Station St', 'hobby': None}
   # 1 {'id': 2, 'name': 'Jane', 'surname': 'Brown', 'address': '8 Parade Rd', 'hobby': 'Digging'}
   # 2 {'id': 3, 'name': 'Okie', 'surname': 'Dokie', 'address': '7 Ocean Rd', 'hobby': None}
   # 3 {'id': 4, 'name': 'Maria', 'surname': None, 'address': '7 Heaven Ave', 'hobby': 'Digging'}

If the table is small, you can use print() function to output it to terminal.

.. code-block:: python

   Person.print()
   #                           Table: Person
   # --------------+---------+-----------+----------------+-----------
   #       id      |   name  |  surname  |    address     |   hobby
   # --------------+---------+-----------+----------------+-----------
   #             1 | John    | Smith     | 1 Station St   | None
   #             2 | Jane    | Brown     | 8 Parade Rd    | Digging
   #             3 | Okie    | Dokie     | 7 Ocean Rd     | None
   #             4 | Maria   | None      | 7 Heaven Ave   | Digging
   #             5 | Bing    | None      | None           | Digging
   # --------------+---------+-----------+----------------+-----------
   # (5 rows)
   


You should notice that all columns now have been normalised for each row, even though all records have not used all column names during insertion.\
When the 1st record (idx 0) is inserted, four columns created. id, name, age and address.
When insert 4th record is inserted (idx 3), a hobby column added.
 
Inspect Person table column list. You can also use function table.get_columns() to list all columns.

.. code-block:: python

   print(bt['Person'])
   # {  
   #   "table name": "Person",  
   #   "columns": [
   #       {"id": 0,"name": "id"},  
   #       {"id": 1,"name": "name"},  
   #       {"id": 2,"name": "age"},  
   #       etc...
   # }

Use update function to change the data. The function signature is table.update(column, value, where=None). The value and where parameters can use lambda function argument for smarter expression.

.. code-block:: python

   bt['Person'].update('age', 10, where=lambda row: row['name']=='Maria') 

Retrieve the row stored at the 3rd index by passing 3 inside the list next to table object.

.. code-block:: python

   bt['Person'][3] 
   #{'id': 4, 'name': 'Maria', 'age': 10, 'address': '7 Heaven Ave', 'hobby': 'Digging'} 



----------------
Common Functions
----------------

We are going to provide some functions that may be needed most when working with Bintang objects.


Bintang.read_excel(path, sheetnames=None)
-----------------------------------------

Read an Excel file and create Bintang tables. The first row will be assumed as column header.
Go to Bintang.Table.read_excel() to read a single sheet and populate the data into created table.

:path: an excel file path to read from.
:sheetname: a list of sheets that only needed to be read. If not specified all available sheets will be read (default).

.. code-block:: python

   bt.read_excel('/path/to/file.xlsx')



Bintang.read_json(json_str, tablepaths=None)
--------------------------------------------
Read JSON string and create a table or more according to hierarchy paths contained in json 'object'.

:json_str: a json string
:tablepaths: a list of paths which contain a list of objects (equivalent to records).

.. code-block:: python
   
   # other module import
   # ...
   import bintang
   import json
   
   # example json data
   json_str = '{"Page:": 100, "Time": "2033-09-05T00:00:00Z", \
               "Person": [{"id": 1, "name": "John", "surname": "Smith", \
                            "Address": {"number": 1, "street": "Station", "street_type": "Street"}}, \
                          {"id": 2, "name": "Jane", "surname": "Brown", \
                            "Address": {"number": 8, "street": "Parade", "street_type": "Road"}}], \
               "PersonDetails": [{"person_id": "1", "hobby": "Blogging", "is_meat_eater": true}, \
                                 {"person_id": "2", "hobby": "Reading", "is_meat_eater": null, \
                                   "LuckyDays": [13, 17, 19]}]}'

   bt = bintang.Bintang('From JSON')
   bt.read_json(json_str)

   print(bt) # show bt tables
   # {
   #    "name": "From JSON",
   #    "tables": [
   #       "/",
   #       "/Person",
   #       "/Person/Address",
   #       "/PersonDetails",
   #       "/PersonDetails/LuckyDays"
   #    ]
   # }

   # loop through root table ('/')
   for idx, row in bt['/'].iterrows():
       print(idx, row)
   0 {'Page:': 100, 'Time': '2033-09-05T00:00:00Z'}

   # loop through  /Person table.
   for idx, row in bt['/Person'].iterrows():
       print(idx, row)
   # 0 {'Person': 0, 'id': 1, 'name': 'John', 'surname': 'Smith'}
   # 1 {'Person': 1, 'id': 2, 'name': 'Jane', 'surname': 'Brown'} 

   # print /Person/Address table. Because this table under /Person, then each record will have their own 
   # reference to /Person table.
   
   bt['/Person/Address'].print()

   #                      Table: /Person/Address
   # -----------+--------------+--------------+-----------+---------------
   #   Address  |    Person    |    number    |   street  |  street_type
   # -----------+--------------+--------------+-----------+---------------
   #  Address   |            0 |            1 | Station   | Street
   #  Address   |            1 |            8 | Parade    | Road
   # -----------+--------------+--------------+-----------+---------------
   # (2 rows)

Please note that since json can contain complex hierarchy paths and still valid (eg. system configuration), then this function may not be in your favour. It might be better to manually extract/locate a certain path manually (hard coded).
   


Bintang.Table.blookup(lkp_table, on, ret_columns)
-------------------------------------------------

Return one or more columns from lookup table.

:lkp_table: lookup table
:on: lookup key tuples
:ret_columns: lookup columns to be returned


.. code-block:: python
    
   # using tables from Example of Usage section above.
   bt['Person'].blookup('FishingClub')], \
       [('name','FirstName')], \
       ['Membership'])

   # check results
   for idx, row in bt['Person'].iterrows(['name','Membership']):
       print(idx, row)

   # 0 {'name': 'John', 'Membership': 'Active'}
   # 1 {'name': 'Jane', 'Membership': 'Active'}
   # 2 {'name': 'Okie', 'Membership': None}
   # 3 {'name': 'Maria', 'Membership': None}    
   
We can see only John and Jane got the membership because their names exists in both tables.
       


Bintang.Table.innerjoin(right_table, on, into, out_leftcolumns, out_rightcolumns)
---------------------------------------------------------------------------------------

return a new table from an inner join operation.

:right_table: name of right table or the second table.
:on: a list of pair columns used for the join.
:into: a new table name to hold the result.
:out_leftcolumns: columns output from left table.
:out_rightcolumns: columns outpout from right table.

.. code-block:: python

   bt.create_table('Person') # This will be a left table
   # insert some record here. See insert below for an example.
   # ...

   bt.create_table('FishingClub') # this will be a right table
   # insert some records here. See insert below for an example.
   # ...

   # let's match the two tables for their firt name and last name.
   res = bt.innerjoin('Person'                                       # left table
                     ,'FishingClub'                                  # right table
                     ,[('name','FirstName'), ('surname','LastName')] # on
                     ,'Fisherman'                                    # into
                     ,out_lcolumns=['name','address']
                     ,out_rcolumns=['Membership']
                     )

   # check the result. you can loop through 'Fisherman' or res.
   for idx, row in bt['Fisherman'].iterrows():
      print(idx, row)



Bintang.Table.insert(record, columns=None)
------------------------------------------
Insert a record into a table.

:record: a list/tuple of data. Or a dict where key=column, value=record
:columns: a list/tuple of columns (in the same order as in the record)

.. code-block:: python

   bt.create_table('Person') 
   p = bt.get_table('Person') # get table object for Person
   # insert data directly from table object instead throug bt object.
   p.insert([1,'John','Smith','1 Station St'], ['id','name','surname','address'])
   p.insert([2,'Jane','Brown','Digging','8 Parade Rd'], ['id','name','surname','hobby','address'])
   p.insert([3,'Okie','Dokie','7 Ocean Rd'], ['id','name','surname','Address'])
   p.insert((4,'Maria','Digging','7 Heaven Ave'), ('id','name','hobby','Address'))
   p.insert((5,'Bing','Digging',None), ('id','name','hobby','Address'))

   bt.create_table('FishingClub')
   # lets make a list of columns so we can pass it to insert.
   columns = ['FirstName','LastName','Membership']
   bt['FishingClub'].insert(['Ajes','Freeman','Active'], columns)
   bt['FishingClub'].insert(['John','Smith','Active'], columns)
   bt['FishingClub'].insert(['John','Brown','Active'], columns)
   bt['FishingClub'].insert(['Okie','Dokie','Active'], columns)
   bt['FishingClub'].insert(['Zekey','Pokey','Active'], columns)


   bt.create_table("Product")
   prod = bt['Product']
   # example of assigning a dictionary argument for record parameter.
   prod.insert({'id':1, 'name':'Hook','price':1.60})
   prod.insert({'id':2, 'name':'Sinker','price':1.20})
   prod.insert({'id':3, 'name':'Reels','price':75})



Bintang.Table.iterrows(columns=None, row_type='dict')
-----------------------------------------------------

Loop through Bintang table's rows and yield index and row. Row can be called out as dict (default) or list.

:columns: a list of columns to output. If None, will output all columns.
:row_type: either 'dict' (default) or 'list'.

.. code-block:: python

   for idx, row in bt['tablename'].iterrows():
       # do something with idx or row
       print(idx, row) 



Bintang.Table.print(columns=None, show_data_type=False)
-------------------------------------------------------

Print rows to terminal in table format. This would be handy if the table can fit into terminal.

:columns: a list of columns to output. If None, will output all columns (default).
:show_data_type: if True, will output data type.

.. code-block:: python

   # assume Person table object exists and has data
   Person.print()



Bintang.Table.read_csv(path, delimiter=',', quotechar='"', header_row=1)
------------------------------------------------------------------------

Read csv file and populate its records to table.

:path: a csv file path to read from.
:delimiter: field seperator, by default it'll accept a comma character.
:header_row: the row number that contains column name or label.

.. code-block:: python

   ## data example in  csv file
   # "id","name","surname","address","hobby"
   # "1","John","Smith","1 Station St",""
   # "2","Jane","Brown","8 Parade Rd","Digging"
   
   bt.create_table('Person')
   bt['Person'].read('/path/to/file.csv') 
   bt['Person'].print()

   #                          Table: Person
   # ------+---------+-----------+----------------+-----------
   #   id  |   name  |  surname  |    address     |   hobby
   # ------+---------+-----------+----------------+-----------
   #  1    | John    | Smith     | 1 Station St   |
   #  2    | Jane    | Brown     | 8 Parade Rd    | Digging
   # ------+---------+-----------+----------------+-----------
   # (2 rows)



Bintang.Table.read_excel(path, sheetname, header_row=1)
-------------------------------------------------------

Read an Excel file into Bintang table.

:path: an excel file path to read from.
:sheetname: the sheetname to read from.
:header_row: the row number that contains column name or label.

.. code-block:: python

   bt.create_table('Person')
   bt['Person'].read_excel('/path/to/file.xlsx', 'Sheet1')
   
   

Bintang.Table.read_sql(conn, sql_str=None, params=None)
-------------------------------------------------------

Read sql table and populate the data to Bintang table.

:conn: pyodbc database connection
:sql_str: sql query, if none it will select * from a same sql table name.
:params: sql parameters

.. code-block:: python

   import bintang
   import pyodbc
   
   # connect to sql server
   conn_str = "DRIVER={ODBC Driver 17 for SQL Server};SERVER=localhost;PORT=1443;DATABASE=test;Trusted_Connection=yes;"
   conn = pyodbc.connect(conn_str)
   sql_str = "SELECT * FROM Person WHERE LastName=?"
   params = ('Dokey')

   bt = bintang.Bintang()
   bt.create_table('Person')
   bt['Person'].read_sql(conn, sql_str, params)

   for idx, row in bt['Person'].iterrows():
       print(idx, row)
       # would print {'ID': 3, 'FirstName': 'Okie', 'LastName': 'Dokey', 'address': '7 Ocean Rd'}

   conn.close()    



Bintang.table.to_csv(path, index=False, delimiter=',', quotechar='"', quoting=0)
--------------------------------------------------------------------------------

Write bintang table to a csv file.

:path: a csv file path to write to.
:index: write row index if it sets True.
:delimiter: field seperator
:quotechar: a character to quote the data
:quoting: the csv enum for quoting, csv.QUOTE_MINIMAL or  0, csv.QUOTE_ALL or 1, csv.QUOTE_NONNUMERIC or 2, csv.QUOTE_NONE or 3

.. code-block:: python

   bt['tablename'].to_csv('/path/to/file.csv')

                  

Bintang.Table.to_excel(path, index=False)
-----------------------------------------

Write Bintang table to an Excel file.

:path: an excel file path to write to.
:index: write row index if it sets True.

.. code-block:: python

   bt['tablename'].to_excel('/path/to/file.xlsx')



Bintang.Table.to_json()
-----------------------
This is just a placeholder. Python make it easy when serializing a dict object to JSON. Conversion would be done by built-in json.JSONEncoder().
Here an example of using our to_dict() function then use build-in module json to convert/export dict to JSON.

.. code-block:: python

   # other modules here
   # ...
   import json
   
   # other codes here
   # ...

   dict_obj = bt['table_name'].to_dict()

   # example to serialise dict_obj to json string
   json_str = json.dumps(dict_obj)
   # use json_str here!
   # ...


   # example to write dict_obj to a json file
   with open ('myfile.json', 'w') as fp:
       json.dump(dict_obj, fp) # this would serialise dict_obj into myfile.json



Bintang.Table.to_sql(conn, schema, table, columns, method='prep', max_rows = 1)
-------------------------------------------------------------------------------

Insert records into sql table.
Notes: Currently tested for SQL Server 2019. However this function should work with other dbms supported by pyodbc.

:conn: pyodbc database connection
:schema: the schema name the sql table belong to.
:table: the table name in the sql database
:columns: a dictionary of column mappings where the key is sql column (destination) and the value is bintang columns (source). If columns is a list, column mapping will be created automatically assuming source columns and destination columns are the same.
:method: 'prep' to use prepared statement (default) or 'string' to use sql string. To avoid sql injection, never use string method when the datasource is external or not known.
:max_rows: maximum rows per insert. Insert more then 1 record when using prep require all data in a column to use the same type, otherwise will raise error.

.. code-block:: python
   
   import bintang
   import pyodbc

   bt = bintang.Bintang('my bintang')
   bt.create_table('Person')
   person = bt.get_table('Person')
   person.insert([1,'John','Smith','1 Station St'], ['id','name','surname','address'])
   person.insert([2,'Jane','Brown','Digging','8 Parade Rd'], ['id','name','surname','address'])
   person.insert([3,'Okie','Dokey','7 Ocean Rd'], ['id','name','surname','address'])
   person.insert((4,'Maria','Digging','7 Heaven Ave'), ('id','name','hobby','Address'))
   person.insert((5,'Bing','Digging',None), ('id','name','hobby','Address'))
    
   # let's map column ID, FirstName, LastName, Address in database to bintang's Person table.
   columns = {'ID':'id', 'FirstName':'name', 'LastName':'surname', 'Address':'address'}
   # connect to database
   conn = pyodbc.connect("DRIVER={ODBC Driver 17 for SQL Server};SERVER=localhost;PORT=1443;DATABASE=test;Trusted_Connection=yes;")  
   # send data to sql
   ret = person.to_sql(conn, 'dbo', 'Person', columns)
   print(f'{ret} record(s) affected.')
   conn.commit()
   conn.close()