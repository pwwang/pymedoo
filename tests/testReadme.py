import helpers, unittest, sqlite3
from medoo import Medoo, Field, Raw, MedooSqlite, Box, Function

class TestReadme(helpers.TestCase):
	
	def dataProvider_testInit(self):
		yield 'sqlite', ':memory:'
	
	def testInit(self, database_type, database_file):
		m = Medoo(database_type = database_type, database_file = database_file)
		self.assertIsInstance(m, MedooSqlite)
		
	def dataProvider_testWhereSelect(self):
		m = Medoo(database_type = 'sqlite', database_file = ':memory:')
		m.create("account", {
			"user_id": 'int',
			"author_id": 'int',
			"user_name": "text",
			"email": "text",
			"age": "int",
			"promoted": "int",
			"gener": "text",
			"password": "text",
			"city": "text",
			"logontime": "text"
		})
		m.create("account2", {
			"replyer_id": 'int',
			"user_id": 'int',
			"user_name": "text",
			"email": "text",
			"age": "int",
			"promoted": "int",
			"gener": "text",
			"password": "text",
			"city": "text"
		})
		m.create("account3", {
			"user_id": 'int',
			"author_id": 'int',
			"user_name": "text",
			"email": "text",
			"age": "int",
			"promoted": "int",
			"gener": "text",
			"password": "text",
		})
		m.create("account4", {
			"author_id": 'int',
			"user_id": 'int',
			"user_name": "text",
			"email": "text",
			"age": "int",
			"promoted": "int",
			"gener": "text",
			"password": "text",
		})
		m.create("post", {
			"id": "int",
			"user_id": "int",
			"author_id": "int",
			"post_id": "int",
			"title": "text",
			"content": "text",
			"restrict": "int",
			"avatar_id": "int",
		})
		m.create("album", {
			"user_id": "int",
		})
		m.create("album2", {
			"user_id": "int",
		})
		
		m.create("photo", {
			"user_id": "int",
			"avatar_id": "int",
		})
		m.create("person", {
			"id": "int",
			"city": "text",
		})
		# 0
		yield m, "account", "user_name", {"email": "foo@bar.com"}, 'SELECT "user_name" FROM "account" WHERE "email" = \'foo@bar.com\''
		yield m, "account", "user_name", {"user_id": 200}, 'SELECT "user_name" FROM "account" WHERE "user_id" = 200'
		yield m, "account", "user_name", {"user_id[>]": 200}, 'SELECT "user_name" FROM "account" WHERE "user_id" > 200'
		yield m, "account", "user_name", {"user_id[>=]": 200}, 'SELECT "user_name" FROM "account" WHERE "user_id" >= 200'
		yield m, "account", "user_name", {"user_id[!]": 200}, 'SELECT "user_name" FROM "account" WHERE "user_id" != 200'
		# 5
		yield m, "account", "user_name", {"age[<>]": (200, 500)}, 'SELECT "user_name" FROM "account" WHERE "age" BETWEEN 200 AND 500'
		yield m, "account", "user_name", {"age[><]": (200, 500)}, 'SELECT "user_name" FROM "account" WHERE "age" NOT BETWEEN 200 AND 500'
		yield m, "account", "user_name", {
			"AND":  {
				"user_name[!]":  "foo",
				"user_id[!]":  1024,
				"email[!]":  ["foo@bar.com", "cat@dog.com", "admin@medoo.in"],
				"promoted[!]":  1
			}
		}, 'SELECT "user_name" FROM "account" WHERE "user_id" != 1024 AND "promoted" != 1 AND "email" NOT IN (\'foo@bar.com\',\'cat@dog.com\',\'admin@medoo.in\') AND "user_name" != \'foo\'', False
		yield m, "account", "user_name", {
			"user_id[>]":  200,
			"age[<>]":  [18, 25],
			"gender":  "female"
		}, 'SELECT "user_name" FROM "account" WHERE "user_id" > 200 AND "age" BETWEEN 18 AND 25 AND "gender" = \'female\'', False
		yield m, "account", "user_name", {
			"OR":  {
				"user_id[>]":  200,
				"age[<>]":  [18, 25],
				"gender":  "female"
			}
		}, 'SELECT "user_name" FROM "account" WHERE "user_id" > 200 OR "age" BETWEEN 18 AND 25 OR "gender" = \'female\'', False
		# 10
		yield m, "account", "*", {
			"AND":  {
				"OR":  {
					"user_name":  "foo",
					"email":  "foo@bar.com"
				},
				"password":  "12345"
			}
		}, 'SELECT * FROM "account" WHERE "password" = \'12345\' AND ("user_name" = \'foo\' OR "email" = \'foo@bar.com\')', False
		yield m, "account", "*", {
			"AND #Actually, this comment feature can be used on every AND and OR relativity condition":  {
				"OR #the first condition":  {
					"user_name":  "foo",
					"email":  "foo@bar.com"
				},
				"OR #the second condition":  {
					"user_name":  "bar",
					"email":  "bar@foo.com"
				}
			}
		}, 'SELECT * FROM "account" WHERE ("user_name" = \'bar\' OR "email" = \'bar@foo.com\') AND ("user_name" = \'foo\' OR "email" = \'foo@bar.com\')', False
		yield m, "post", ["post.id", "post.content"], {
			"AND":  {
				"post.restrict[<]": Field("account.age") + 1,				
				"account.user_name":  "foo",
				"account.email":  "foo@bar.com",
			}
		}, 'SELECT "post"."id","post"."content" FROM "post" LEFT JOIN "account" ON "account"."user_id"="post"."author_id" WHERE "account"."user_name" = \'foo\' AND "post"."restrict" < "account"."age"+1 AND "account"."email" = \'foo@bar.com\'', False, {
			"[>]account":  {"user_id": "author_id"},
		}
		yield m, "person", "id", {"city[~]": ["lon", "foo", "bar"]}, 'SELECT "id" FROM "person" WHERE "city" LIKE \'%lon%\' OR "city" LIKE \'%foo%\' OR "city" LIKE \'%bar%\''
		yield m, "person", "id", {"city[!~]": "lon"}, 'SELECT "id" FROM "person" WHERE "city" NOT LIKE \'%lon%\''
		# 15
		yield m, "person", "id", {"city[!~]": ["lon", "foo", "bar"]}, 'SELECT "id" FROM "person" WHERE "city" NOT LIKE \'%lon%\' AND "city" NOT LIKE \'%foo%\' AND "city" NOT LIKE \'%bar%\''
		# joining
		yield m, "post", [
			"post.post_id",
			"post.title",
			"account.user_id",
			"account.city",
			"replyer.user_id",
			"replyer.city"
		], {
			"post.user_id":  100,
			"ORDER":  {"post.post_id":  "DESC"},
			"LIMIT":  50
		}, 'SELECT "post"."post_id","post"."title","account"."user_id","account"."city","replyer"."user_id","replyer"."city" FROM "post" LEFT JOIN "account4" ON "account4"."author_id"="post"."user_id" AND "album"."user_id"="post"."user_id" LEFT JOIN "album2" ON "account"."user_id"="post"."user_id" LEFT JOIN "account2" "replyer" ON "replyer"."replyer_id"="post"."user_id" LEFT JOIN "account3" ON "account3"."author_id"="post"."user_id" LEFT JOIN "photo" USING ("user_id","avatar_id") LEFT JOIN "album" USING ("user_id") LEFT JOIN "account" ON "account"."author_id"="post"."user_id" WHERE "post"."user_id" = 100 ORDER BY "post"."post_id" DESC LIMIT 1,50', False, {
			"[>]account":  {"author_id":  "user_id"},
			"[>]album":  "user_id",
			"[>]photo":  ["user_id", "avatar_id"],
			"[>]account2 (replyer)":  {"replyer_id":  "user_id"},
			"[>]account3":  {"author_id":  "user_id"},
			"[>]album2":  {"account.user_id":  "user_id"},
			"[>]account4":  {
				"author_id":  "user_id",
				"album.user_id":  "user_id"
			}
		}
		yield m, "account", Function.count(Function.distinct('user_name'), alias = 'count'), {'id[<]': 10}, 'SELECT COUNT(DISTINCT "user_name") "count" FROM "account" WHERE "id" < 10'
		yield m, "account", 'user_name(user)', {'logontime[<]': Raw("date('now')")}, """SELECT "user_name" "user" FROM "account" WHERE "logontime" < date('now')"""
		yield m, "account", Field('user_name'), {'email': 'foo@bar.com'}, """SELECT "user_name" FROM "account" WHERE "email" = 'foo@bar.com'"""
		yield m, 'account', '*', {'email[~]': Raw("""'%' || "user_name" || '%'""")}, """SELECT * FROM "account" WHERE "email" LIKE '%' || "user_name" || '%'"""

	def testWhereSelect(self, m, table, columns, where, sql, exact = True, join = None):
		m.select(table, columns = columns, where = where, join = join)
		rsql = m.last()
		if exact:
			self.assertEqual(rsql, sql)
		else:
			self.assertItemEqual(list(rsql), list(sql))
	
			
	def dataProvider_testInsert(self):
		m = Medoo(database_type = 'sqlite', database_file = ':memory:')
		m.create('account', {
			"id": "int primary key",
			"user_name": "text", 
			"email": "text", 
			"age": "int",
			"lang": "text"
		})
		yield m, "account", (1, "Bob Smith", 'foo@bar.com', 25, ["en", "fr"]), [(2, 'Michael Jodan', 'bar@foo.com', 33, ['jp', 'cn'])], """INSERT INTO "account" VALUES (1,'Bob Smith','foo@bar.com',25,'[''en'', ''fr'']'), (2,'Michael Jodan','bar@foo.com',33,'[''jp'', ''cn'']')"""
		yield m, "account", {
			"user_name":  "foo",
			"email":  "foo@bar.com",
			"age":  25,
			"lang[json]":  ["en", "fr"] #:  '["en", "fr"]'
		}, [{
			"user_name":  "bar",
			"email":  "bar@foo.com",
			"age":  33,
			"lang[json]":  ["jp", "cn"] #:  '["jp", "cn"]'
		}], """INSERT INTO "account" ("lang","age","user_name","email") VALUES ('[''en'', ''fr'']',25,'foo','foo@bar.com'), ('[''jp'', ''cn'']',33,'bar','bar@foo.com')""", False
			
	def testInsert(self, m, table, data, datas, sql, exact = True):
		m.insert(table, data, *datas)
		if exact:
			self.assertEqual(m.last(), sql)
		else:
			self.assertItemEqual(list(m.last()), list(sql))
	
	def dataProvider_testUpdate(self):
		m = Medoo(database_type = 'sqlite', database_file = ':memory:')
		fields = Box()
		fields.user_id = 'int'
		fields.type = 'text'
		fields.age = 'int'
		fields.level = 'int'
		fields.score = 'int'
		fields.lang = 'text'
		m.create('account', fields)
		m.insert('account', (1, 'user', 24, 10, 50, None))
		yield m, 'account', {
			"type": "user",
			"age[+]": 1,
			"level[-]": 5,
			"score[*]": 2,
			"lang": ["en", "fr", "jp", "cn"]
		}, {'user_id[<]': 1000}, """UPDATE "account" SET "lang"='[''en'', ''fr'', ''jp'', ''cn'']\',"age"="age"+1,"level"="level"-5,"type"='user',"score"="score"*2 WHERE "user_id" < 1000""", False
	
	def testUpdate(self, m, table, data, where, sql, exact):
		m.update(table, data, where)
		if exact:
			self.assertEqual(m.last(), sql)
		else:
			self.assertItemEqual(list(m.last()), list(sql))
		
if __name__ == '__main__':
	unittest.main(verbosity = 2)

