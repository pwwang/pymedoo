import helpers, unittest, sqlite3
from medoo import Medoo, Field, Raw, MedooSqlite

class TestReadme(helpers.TestCase):
	
	def dataProvider_testInit(self):
		yield 'sqlite', ':memory:'
	
	def testInit(self, database_type, database_file):
		m = Medoo(database_type = database_type, database_file = database_file)
		self.assertIsInstance(m, MedooSqlite)
		
	def dataProvider_testWhereSelect(self):
		# 0
		yield "account", "user_name", {"email": "foo@bar.com"}, 'SELECT "user_name" FROM "account" WHERE "email" = \'foo@bar.com\''
		yield "account", "user_name", {"user_id": 200}, 'SELECT "user_name" FROM "account" WHERE "user_id" = 200'
		yield "account", "user_name", {"user_id[>]": 200}, 'SELECT "user_name" FROM "account" WHERE "user_id" > 200'
		yield "account", "user_name", {"user_id[>=]": 200}, 'SELECT "user_name" FROM "account" WHERE "user_id" >= 200'
		yield "account", "user_name", {"user_id[!]": 200}, 'SELECT "user_name" FROM "account" WHERE "user_id" != 200'
		# 5
		yield "account", "user_name", {"age[<>]": (200, 500)}, 'SELECT "user_name" FROM "account" WHERE "age" BETWEEN 200 AND 500'
		yield "account", "user_name", {"age[><]": (200, 500)}, 'SELECT "user_name" FROM "account" WHERE "age" NOT BETWEEN 200 AND 500'
		yield "account", "user_name", {
			"AND":  {
				"user_name[!]":  "foo",
				"user_id[!]":  1024,
				"email[!]":  ["foo@bar.com", "cat@dog.com", "admin@medoo.in"],
				"promoted[!]":  1
			}
		}, 'SELECT "user_name" FROM "account" WHERE "user_id" != 1024 AND "promoted" != 1 AND "email" NOT IN (\'foo@bar.com\',\'cat@dog.com\',\'admin@medoo.in\') AND "user_name" != \'foo\'', False
		yield "account", "user_name", {
			"user_id[>]":  200,
			"age[<>]":  [18, 25],
			"gender":  "female"
		}, 'SELECT "user_name" FROM "account" WHERE "user_id" > 200 AND "age" BETWEEN 18 AND 25 AND "gender" = \'female\'', False
		yield "account", "user_name", {
			"OR":  {
				"user_id[>]":  200,
				"age[<>]":  [18, 25],
				"gender":  "female"
			}
		}, 'SELECT "user_name" FROM "account" WHERE "user_id" > 200 OR "age" BETWEEN 18 AND 25 OR "gender" = \'female\'', False
		# 10
		yield "account", "*", {
			"AND":  {
				"OR":  {
					"user_name":  "foo",
					"email":  "foo@bar.com"
				},
				"password":  "12345"
			}
		}, 'SELECT * FROM "account" WHERE "password" = \'12345\' AND ("user_name" = \'foo\' OR "email" = \'foo@bar.com\')'
		yield "account", "*", {
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
		}, 'SELECT * FROM "account" WHERE ("user_name" = \'bar\' OR "email" = \'bar@foo.com\') AND ("user_name" = \'foo\' OR "email" = \'foo@bar.com\')'
		yield "post", ["post.id", "post.content"], {
			"AND":  {
				"post.restrict[<]": Field("account.age"),				
				"account.user_name":  "foo",
				"account.email":  "foo@bar.com",
			}
		}, 'SELECT "post"."id","post"."content" FROM "post" LEFT JOIN "account" ON "account"."user_id"="post"."author_id" WHERE "account"."user_name" = \'foo\' AND "post"."restrict" < "account"."age" AND "account"."email" = \'foo@bar.com\'', True, {
			"[>]account":  {"user_id": "author_id"},
		}
		yield "person", "id", {"city[~]": ["lon", "foo", "bar"]}, 'SELECT "id" FROM "person" WHERE "city" LIKE \'%lon%\' OR "city" LIKE \'%foo%\' OR "city" LIKE \'%bar%\''
		yield "person", "id", {"city[!~]": "lon"}, 'SELECT "id" FROM "person" WHERE "city" NOT LIKE \'%lon%\''
		# 15
		yield "person", "id", {"city[!~]": ["lon", "foo", "bar"]}, 'SELECT "id" FROM "person" WHERE "city" NOT LIKE \'%lon%\' AND "city" NOT LIKE \'%foo%\' AND "city" NOT LIKE \'%bar%\''
		# joining
		yield "post", [
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
		}, 'SELECT "post"."post_id","post"."title","account"."user_id","account"."city","replyer"."user_id","replyer"."city" FROM "post" LEFT JOIN "account4" ON "account4"."author_id"="post"."user_id" AND "album"."user_id"="post"."user_id" LEFT JOIN "album2" ON "account"."user_id"="post"."user_id" LEFT JOIN "account2" "replyer" ON "replyer"."replyer_id"="post"."user_id" LEFT JOIN "account3" ON "account3"."author_id"="post"."user_id" LEFT JOIN "photo" ON "photo"."user_id"="post"."user_id" AND "photo"."avatar_id"="post"."avatar_id" LEFT JOIN "album" ON "album"."user_id"="post"."user_id" LEFT JOIN "account" ON "account"."author_id"="post"."user_id" WHERE "post"."user_id" = 100 ORDER BY "post"."post_id" DESC LIMIT 1,50', True, {
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
		
	
	def testWhereSelect(self, table, columns, where, sql, exact = True, join = None):
		m = Medoo(database_type = 'sqlite', database_file = ':memory:')
		m.select(table, columns = columns, where = where, join = join)
		rsql = m.last()
		if exact:
			self.assertEqual(rsql, sql)
		else:
			self.assertItemsEqual(list(rsql), list(sql))
		
if __name__ == '__main__':
	unittest.main(verbosity = 2)

