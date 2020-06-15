# pymedoo - A lightweight database framework for python.
it's inspired by [Medoo][1] for PHP and [Records][7] for python.

[![Pypi][8]][15]
[![Github][9]][16]
[![Codacy][10]][17]
[![Codacy coverage][11]][17]
[![Travis building][12]][18]

## Install
```bash
pip install medoo
```

## Required packages for databases

| Database | Package   |
|----------|---------  |
| sqlite   | [sqlite3][2]   |
| mysql    | ~~[pymysql][3]~~(dropped) Use [mysql.connector][13] instead. See [#6][14] |
| pgsql    | [psycopg2][4]  |
| mssql    | [pymssql][5]   |
| oracle   | [cx_Oracle][6] |

## Get started
### SELECT

```python
from medoo import Medoo

# For other arguments, please refer to the original connect function of each client.
me = Medoo(dbtype = 'sqlite', database = 'file:///path/to/test.sqlite')

# SELECT * FROM "Customers"
rs = me.select('Customers')

print(rs.export('csv', delimiter = '\t'))
```
|CustomerID|CustomerName|ContactName|Address|City|PostalCode|Country|
|-|-|-|-|-|-|-|
|1|Alfreds Futterkiste|Maria Anders|Obere Str. 57|Berlin|12209|Germany|
|2|Ana Trujillo Emparedados y helados|Ana Trujillo|Avda. de la Constitución 2222|México D.F.|5021|Mexico|
|3|Antonio Moreno Taquería|Antonio Moreno|Mataderos 2312|México D.F.|5023|Mexico|
|4|Around the Horn|Thomas Hardy|120 Hanover Sq.|London|WA1 1DP|UK|
|5|Berglunds snabbköp|Christina Berglund|Berguvsvägen 8|Luleå|S-958 22|Sweden|

```python
# SELECT "CustomerID","CustomerName" FROM "Customers"
me.select('Customers', 'CustomerID, CustomerName')
me.select('Customers', ['CustomerID', 'CustomerName'])

# SELECT "C"."CustomerID" AS "CustomerID","C"."CustomerName" AS "name" FROM "Customers" AS "C"
me.select('Customers(C)', ['C.CustomerID(id)', 'C.CustomerName(name)'])

# SELECT DISTINCT "Country" FROM "Customers"
me.select('Customers', 'Country', distinct = True)

# SELECT COUNT("CustomerID") FROM "Customers"
me.select('Customers', 'CustomerID|COUNT')

# SELECT COUNT(DISTINCT "CustomerID") AS "c" FROM "Customers"
me.select('Customers', 'CustomerID|.COUNT(c)')

# SELECT "CustomerID"+1 FROM "Customers"
from medoo import Field, Raw
me.select('Customers', Field('CustomerID')+1)

# SELECT 'Name: ' || CustomerName AS name FROM "Customers"
rs = me.select('Customers', Raw("'Name: ' || CustomerName AS name"))
for r in rs: print(r.name)
```
```
Name: Alfreds Futterkiste
Name: Ana Trujillo Emparedados y helados
Name: Antonio Moreno Taquería
Name: Around the Horn
Name: Berglunds snabbköp
```

### WHERE
#### Single condition
```python
# SELECT * FROM "Customers" WHERE "CustomerID" = 1
me.select('Customers', where = {'CustomerID': 1})

# SELECT * FROM "Customers" WHERE "CustomerID" < 3
me.select('Customers', where = {'CustomerID[<]': 3})

# SELECT * FROM "Customers" WHERE "CustomerID" IN (1,2,3)
me.select('Customers', where = {'CustomerID': (1,2,3)})

# SELECT * FROM "Customers" WHERE "CustomerName" LIKE '%b%' OR "CustomerName" LIKE '%c%'
me.select('Customers', where = {'CustomerName[~]': ('a', 'b')})

# SELECT * FROM "Customers" WHERE "CustomerID" BETWEEN 1 AND 3
me.select('Customers', where = {'CustomerID[<>]': (1,3)})

# SELECT * FROM "Customers" WHERE NOT "CustomerID" BETWEEN 1 AND 3
me.select('Customers', where = {'!CustomerID[<>]': (1,3)})

# SELECT * FROM "Customers" WHERE "CustomerID" IS NULL
me.select('Customers', where = {'CustomerID[is]': None}) # where = {'id[==]': None}

# SELECT * FROM "Customers" WHERE INSTR("CustomerName", 'Antonio')
me.select('Customers', where = {Raw('INSTR("CustomerName", \'Antonio\')'):None})
```

#### Compond
```python
# SELECT * FROM "Customers" WHERE "CustomerID" IN (1,2,3) AND "CustomerName" LIKE '%b%'
me.select('Customers', where = {
    'CustomerID': (1,2,3),
    'CustomerName[~]': 'b'
})
# SELECT * FROM "Customers"
# WHERE ("CustomerID" IN (1,2,3) AND "CustomerName" LIKE '%b%') AND
#	("CustomerName" = 'cd' OR "CustomerID" = 2) AND
#	("CustomerID" < 3 AND NOT "CustomerName" = 'bc')
me.select('Customers', where = {
    'AND': {
        'CustomerID': (1,2,3),
        'CustomerName[~]': 'b'
    },
    'OR': {
        'CustomerName': 'cd',
        'CustomerID': 2
    },
    # you can use comment to distinguish multiple ANDs and ORs
    'AND #2': {
        'CustomerID[<]': 3,
        '!CustomerName': 'bc'
    }
})
```

#### Modifier
```python
# SELECT * FROM "Customers" ORDER BY "CustomerID" DESC, "CustomerName" ASC LIMIT 2 OFFSET 1
# MSSQL:
# SELECT * FROM "Customers" ORDER BY "CustomerID" DESC, "CustomerName" ASC
#	OFFSET 1 ROWS FETCH NEXT 2 ROWS ONLY
me.select('Customers', where = {
    'ORDER': {'CustomerID': 'desc', 'CustomerName': 'asc'},
    'LIMIT': (2, 1)
})

# SELECT COUNT("CustomerID") AS "c","CustomerName" FROM "Customers" GROUP BY "Country" HAVING "CustomerID" > 1
me.select('Customers', 'CustomerID|count(c), CustomerName', where = {
    'GROUP': 'Country',
    'HAVING': {'CustomerID[>]': 1}
})
```

### Using subquery
```python
print(me.select('Orders').export('csv', delimiter = '\t'))
```
|OrderID|CustomerID|OrderDate|
|-|-|-|
|10308|2|1996-09-18|
|10309|37|1996-09-19|
|10310|77|1996-09-20|
```python
# SELECT * FROM "Customers" AS "C",(SELECT "CustomerID" FROM "Orders") AS "O"
#   WHERE "C"."CustomerID" = "O"."CustomerID"
me.select([
    'Customers(C)', # the first table
    me.builder.select('Orders', 'CustomerID', sub = 'O')
], where = {
    'C.CustomerID': Field('O.CustomerID')
})

# SELECT * FROM "Customers" WHERE "CustomerID" IN (SELECT "CustomerID" FROM "Orders")
me.select('Customers', where = {
    'CustomerID': me.builder.select('Orders', 'CustomerID')
})
```

### JOIN
```python
# SELECT "O"."OrderID","C"."CustomerName","O"."OrderDate" FROM "Orders" AS "O"
#   INNER JOIN "Customers" AS "C" ON "C"."CustomerID"="O"."CustomerID"
me.select('Orders(O)', 'O.OrderID,C.CustomerName,O.OrderDate', join = {
    'Customers(C)': 'CustomerID'
})

# equivalent to
me.select('Orders(O)', 'O.OrderID,C.CustomerName,O.OrderDate', join = {
    'Customers(C)[><]': 'CustomerID'
})
# [>] LEFT JOIN, [<] RIGHT JOIN [<>] FULL OUTER JOIN

# Join on multiple columns (same in different tables)
# join = { 'Customers(C)[><]': ['CustomerID', 'OtherColumn'] }

# Join on different columns: JOIN "Customers" AS "C" ON "C"."CustomerID"="O"."OtherID"
# join = { 'Customers(C)[><]': {'CustomerID', 'OtherID'} }

# You can join multiple tables, use OrderedDict if you want to keep the order.
```

### UNION
```python
# SELECT "CustomerID" FROM "Customers" UNION SELECT "CustomerID" FROM "Orders"
me.union(
    me.builder.select('Customers', 'CustomerID'),
    me.builder.select('Orders', 'CustomerID')
)

# SELECT "CustomerID" FROM "Customers" UNION ALL SELECT "CustomerID" FROM "Orders"
me.union(
    me.builder.select('Customers', 'CustomerID'),
    me.builder.select('Orders', 'CustomerID', sub = True)
)
```

### Records
`Medoo.select` and `Medoo.union` return a collection of records, which is basically a generator, but you can still get items from it, as it will consume the generate if necessary. The idea is borrowed from [Records][7].
```python
records = me.select('Customers', 'CustomerID(id)')
record  = records.first() # <Record {'id': 1}>

# equivalent to
record  = records[0]

# you may also select other rows: records[1], records[2]
# or return all rows:
print(records.all())

# you can also export the records
# this is the courtesy from tablib (https://github.com/kennethreitz/tablib)
# check the kwargs with its documentation
print(records.export('csv', delimiter = '\t'))

# You can also apply tablib's other function on the data:
# records.tldata.<function>(<args>)

# to get the value of each field from a record:
print(record[0]) # 1
print(record['id']) # 1
print(record.id) # 1
print(record.as_dict()) # {'id': 1}
```

### INSERT
```python
# INSERT INTO "Orders" ("OrderID","CustomerID","OrderDate") VALUES (1,2,'1999-09-09'),(2,8,'2001-10-12')
me.insert(
    'Orders', # table
    'OrderID, CustomerID, OrderDate', # fields
    (1,2,'1999-09-09'), # values
    (2,8,'2001-10-12')
    # ...
)
# get the last insert row id
print(me.id()) # 5

# INSERT INTO "Orders" ("OrderID","CustomerID","OrderDate") VALUES (1,2,'1999-09-09'),(2,8,'2001-10,12')
me.insert(
    'Orders', # table
    {'OrderID': 1, 'CustomerID': 2, 'OrderDate': '1999-09-09'}, # fields with the first value
    (2,8,'2001-10-12')
    # ...
)
me.insert(
    'Orders', # table
    {'OrderID': 1, 'CustomerID': 2, 'OrderDate': '1999-09-09'}, # fields with the first value
    {'OrderID': 2, 'CustomerID': 8, 'OrderDate': '2001-10-12'}  # specify the fields as well
    # ...
)
# Or if your values have all the fields
# INSERT INTO "Orders" VALUES (1,2,'1999-09-09'),(2,8,'2001-10-12')
me.insert(
    'Orders', # table
    (1,2,'1999-09-09')
    (2,8,'2001-10-12')
    # ...
)

# You may hold the changes until all data inserted
me.insert(..., commit = False)
me.insert(..., commit = False)
me.insert(..., commit = False)
me.insert(..., commit = False)
me.commit()
# This applies with UPDATE and DELETE as well.
```

### UPDATE
```python
# UPDATE "Orders" SET "CustomerID"=10 WHERE "OrderID" = 2
me.update(
    'Orders', # table
    data  = {'CustomerID': 10},
    where = {'OrderID': 2}
)
# UPDATE "Orders" SET "CustomerID"="CustomerID"+1 WHERE "OrderID" = 2
me.update(
    'Orders', # table
    data  = {'CustomerID[+]': 1},
    where = {'OrderID': 2}
)
```

### DELETE
```python
# DELETE FROM "Orders" WHERE "OrderID" = 2
me.delete('Orders', where = {'OrderID': 2})
```

### Other functions of `Medoo`
```python
# Fetch a single value
me.get('Customers', 'CustomerID', where = {'CustomerName': 'Around the Horn'}) # == 1

# Check if a record exists
me.has('Customers', where = {'CustomerID': 10}) # == False

# Return the last query
me.last() # SELECT * FROM "Customers" WHERE "CustomerID" = 10

# Show all the queries bound with `me`

# You have to passing `logging = True` to `Medoo(..., logging = True)`
me.log()

# Return the errors
me.error()

# Submit an SQL query
me.query(sql, commit = True)
```

### Extending `pymedoo`
`pymedoo` is highly extendable, including the operators in `WHERE` conditions and `UPDATE SET` clause, `JOIN` operators, and some functions such as how to quote the table names, field names and values. All of these have been defined with `Dialect` class, what you need to do is just extend this class and specify it to the `Medoo` instance.
For example, let's define a case-insensitive `LIKE` operator using a shortcut `~~`:
```python
from medoo import Medoo, Dialect

class MyDialect(Dialect):
    OPERATOR_MAP = {
        '~~': 'ilike'
    }

    @classmethod
    def ilike(klass, field, value):
        # support single value
        if not isinstance(value, list):
            value = [value]

        terms = [
            "UPPER({}) LIKE UPPER({})".format(field, klass.value(v)) # quote the value
            for v in value
        ]
        # use OR to connect
        return ' OR '.join(terms)

# tell medoo to use this dialect
me = Medoo(...)
me.dialect(MyDialect)

# SELECT * FROM "Customers" WHERE UPPER("CustomerName") LIKE UPPER('%an%')
records = me.select('Customers', where = {
    'CustomerName[~~]': '%an%'
})
print(records.export('csv', delimiter = '\t'))
```
|CustomerID|CustomerName|ContactName|Address|City|PostalCode|Country|
|-|-|-|-|-|-|-|
|2|Ana Trujillo Emparedados y helados|Ana Trujillo|Avda. de la Constitución 2222|México D.F.|5021|Mexico|
|3|Antonio Moreno Taquería|Antonio Moreno|Mataderos 2312|México D.F.|5023|Mexico|


[1]: https://medoo.in/
[2]: https://docs.python.org/2/library/sqlite3.html
[3]: https://github.com/PyMySQL/PyMySQL
[4]: http://initd.org/psycopg/docs/
[5]: http://www.pymssql.org/en/stable/
[6]: https://oracle.github.io/python-cx_Oracle/
[7]: https://github.com/kennethreitz/records
[8]: https://img.shields.io/pypi/v/pymedoo.svg?style=flat-square
[9]: https://img.shields.io/github/tag/pwwang/pymedoo.svg?style=flat-square
[10]: https://img.shields.io/codacy/grade/83a79e32a9414a08be67d17b3e93a2ad.svg?style=flat-square
[11]: https://img.shields.io/codacy/coverage/83a79e32a9414a08be67d17b3e93a2ad.svg?style=flat-square
[12]: https://img.shields.io/travis/pwwang/pymedoo.svg?style=flat-square
[13]: https://dev.mysql.com/doc/connector-python/en/
[14]: https://github.com/pwwang/pymedoo/issues/6
[15]: https://pypi.org/project/pymedoo/
[16]: https://github.com/pwwang/pymedoo
[17]: https://app.codacy.com/manual/pwwang/pymedoo
[18]: https://travis-ci.org/github/pwwang/pymedoo
