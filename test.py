
from medoo import Medoo
db = Medoo(dbtype='mysql', database='books', host='localhost', user='root', password='', charset='utf8')
rs = db.select('test', '*', {'LIMIT': 3})
# db.insert('test', {'title': 'ccc', 'priority': 1, 'status': 1})
for r in rs:
        print(r.task_id)
