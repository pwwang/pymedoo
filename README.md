# pymedoo
A lightweight database framework for python inspired by [Medoo][1].  
Note: currently only sqlite is supported.

## Dependency:
- [pypika][2]

## Install
```bash
pip install git+git://github.com/pwwang/pymedoo.git
```

## Initialize
```python
from medoo import Medoo

m = Medoo(database_type = 'sqlite', database_file = ':memory:')
```

##  Where
### Basic condition
```python
m.select("account", columns = "user_name", where = {
	"email": "foo@bar.com"
})
# WHERE email = 'foo@bar.com'

m.select("account", columns = "user_name", where = {
	"user_id": 200
})
# WHERE user_id = 200
 
m.select("account", columns = "user_name", where = {
	"user_id[>]": 200
})
# WHERE user_id > 200
 
m.select("account", columns = "user_name", where = {
	"user_id[>=]": 200
})
# WHERE user_id >= 200
 
m.select("account", columns = "user_name", where = {
	"user_id[!]": 200
})
# WHERE user_id != 200
 
m.select("account", columns = "user_name", where = {
	"age[<>]":  [200, 500]
})
# WHERE age BETWEEN 200 AND 500
 
# [Negative condition]
m.select("account", columns = "user_name", where = {
	"age[><]":  [200, 500]
})
# WHERE age NOT BETWEEN 200 AND 500

m.select("account", columns = "user_name", where = {
	"AND":  [
		"user_name[!]":  "foo",
		"user_id[!]":  1024,
		"email[!]":  ["foo@bar.com", "cat@dog.com", "admin@medoo.in"],
		"promoted[!]":  True
	]
})
# WHERE
# `user_name` != 'foo' AND
# `user_id` != 1024 AND
# `email` NOT IN ('foo@bar.com','cat@dog.com','admin@medoo.in') AND
# `city` IS NOT NULL
# `promoted` != 1
```

### Relative condition
```python
m.select("account", columns = "user_name", where = {
	"AND":  {
		"user_id[>]":  200,
		"age[<>]":  [18, 25],
		"gender":  "female"
	}
})
 
# Medoo will connect relativity condition with AND by default. The following usage is the same like above.
m.select("account", columns = "user_name", where = {
	"user_id[>]":  200,
	"age[<>]":  [18, 25],
	"gender":  "female"
})
 
# WHERE user_id > 200 AND age BETWEEN 18 AND 25 AND gender = 'female'
 
m.select("account", columns = "user_name", where = {
	"OR":  {
		"user_id[>]":  200,
		"age[<>]":  [18, 25],
		"gender":  "female"
	}
})
# WHERE user_id > 200 OR age BETWEEN 18 AND 25 OR gender = 'female'
```

### Compound
```python
m.select("account", columns = '*', where = {
	"AND":  {
		"OR":  {
			"user_name":  "foo",
			"email":  "foo@bar.com"
		},
		"password":  "12345"
	}
})
# WHERE (user_name = 'foo' OR email = 'foo@bar.com') AND password = '12345'
 
# [IMPORTANT]
# Because Medoo is using dict data construction to describe relativity condition,
# array with duplicated key will be overwritten.
#
# This will be error:
m.select("account", columns = '*', where = {
	"AND":  {
		"OR":  {
			"user_name":  "foo",
			"email":  "foo@bar.com"
		},
		"OR":  {
			"user_name":  "bar",
			"email":  "bar@foo.com"
		}
	}
})
# [X] SELECT * FROM "account" WHERE ("user_name" = 'bar' OR "email" = 'bar@foo.com')
 
# To correct that, just assign a comment for each AND and OR key name. The comment content can be everything.
m.select("account", columns = '*', where = {
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
})
# SELECT * FROM "account"
# WHERE (
# 	(
# 		"user_name" = 'foo' OR "email" = 'foo@bar.com'
# 	)
# 	AND
# 	(
# 		"user_name" = 'bar' OR "email" = 'bar@foo.com'
# 	)
# )
```

### Columns Relationship
```python
m.select("post", join = {
		"[>]account":  ["author_id":  "user_id"],
	}, columns = {
		"post.id",
		"post.content"
	}, where = {
		"AND":  {
			# Connect two column with condition sign like [=], [>], [<], [!=] as one of array value
			"post.restrict[<]account.age",
			
			"account.user_name":  "foo",
			"account.email":  "foo@bar.com",
		}
	}
)
 
# WHERE "post"."restrict" < "account"."age" AND "account"."user_name" = 'foo' AND "account"."email" = 'foo@bar.com'
```

### LIKE Condition
```python
# By default, the keyword will be quoted with % front and end to match the whole word.
m.select("person", columns = "id", where = {
	"city[~]":  "lon"
})
 
WHERE "city" LIKE '%lon%'
 
# Array support
m.select("person", columns = "id", where = {
	"city[~]":  ["lon", "foo", "bar"]
})
 
WHERE "city" LIKE '%lon%' OR "city" LIKE '%foo%' OR "city" LIKE '%bar%'
 
# Negative condition [!~]
m.select("person", columns = "id",  where = {
	"city[!~]":  "lon"
})
 
WHERE "city" NOT LIKE '%lon%'
```

## Select
### Basic select
```python
datas = m.select("account", columns = [
	"user_name",
	"email"
], where = {
	"user_id[>]":  100
})
 
# datas = [
# 	{
# 		"user_name":  "foo",
# 		"email":  "foo@bar.com"
# 	},
# 	{
# 		"user_name":  "cat",
# 		"email":  "cat@dog.com"
# 	}
# ]
 
for data in datas:
	print "user_name: " + data.user_name + " - email: " + data.email
 
# Select all columns
datas = m.select("account", columns = "*")
 
# Select a column
datas = m.select("account", columns = "user_name")
 
# datas = {
# 	"user_name":  "foo",
# 	"user_name":  "cat"
# }
```

### Table joining
```python
# [>] == LEFT JOIN
# [<] == RIGH JOIN
# [<>] == FULL JOIN
# [><] == INNER JOIN
 
m.select("post", [
	# Here is the table relativity argument that tells the relativity between the table you want to join.
 
	# The row author_id from table post is equal the row user_id from table account
	"[>]account":  ["author_id":  "user_id"],
 
	# The row user_id from table post is equal the row user_id from table album.
	# This is a shortcut to declare the relativity if the row name are the same in both table.
	"[>]album":  "user_id",
 
	# [post.user_id is equal photo.user_id and post.avatar_id is equal photo.avatar_id]
	# Like above, there are two row or more are the same in both table.
	"[>]photo":  ["user_id", "avatar_id"],
 
	# If you want to join the same table with different value,
	# you have to assign the table with alias.
	"[>]account (replyer)":  ["replyer_id":  "user_id"],
 
	# You can refer the previous joined table by adding the table name before the column.
	"[>]account":  ["author_id":  "user_id"],
	"[>]album":  ["account.user_id":  "user_id"],
 
	# Multiple condition
	"[>]account":  [
		"author_id":  "user_id",
		"album.user_id":  "user_id"
	]
], [
	"post.post_id",
	"post.title",
	"account.user_id",
	"account.city",
	"replyer.user_id",
	"replyer.city"
], {
	"post.user_id":  100,
	"ORDER":  ["post.post_id":  "DESC"],
	"LIMIT":  50
})
 
# SELECT
# 	`post`.`post_id`,
# 	`post`.`title`,
# 	`account`.`city`
# FROM `post`
# LEFT JOIN `account` ON `post`.`author_id` = `account`.`user_id`
# LEFT JOIN `album` USING (`user_id`)
# LEFT JOIN `photo` USING (`user_id`, `avatar_id`)
# WHERE
# 	`post`.`user_id` = 100
# ORDER BY `post`.`post_id` DESC
# LIMIT 50
```

## Insert
```python
m.insert("account", {
	"user_name":  "foo",
	"email":  "foo@bar.com",
	"age":  25,
	"lang":  ["en", "fr", "jp", "cn"] #: '[\'en\', \'fr\', \'jp\', \'cn\']'
})
 
m.insert("account", {
	"user_name":  "foo",
	"email":  "foo@bar.com",
	"age":  25,
	"lang[JSON]":  ["en", "fr", "jp", "cn"] #:  '["en","fr","jp","cn"]'
})
```

## Update
```python
m.update("account", {
	"type":  "user",
 
	# All age plus one
	"age[+]":  1,
 
	# All level subtract 5
	"level[-]":  5,
 
	# All score multiplied by 2
	"score[*]":  2,
 
	# Array value
	"lang":  ["en", "fr", "jp", "cn"],
 
	# Array value encoded as JSON
	"lang [JSON]":  ["en", "fr", "jp", "cn"],
}, {
	"user_id[<]":  1000
})
```

## Delete
```python
m.delete("account", {
	"AND":  {
		"type":  "business",
		"age[<]":  18
	}
})
 
# The return object of delete() is PDOStatement, so you can use its methods to get more information. 
m.delete("account", {
	"AND":  {
		"type":  "business",
		"age[<]":  18
	}
})
```

## Error
```python
m.select("account", None, '* WHERE', {
	"user_id[<]":  20
})
 
print m.error()
 
# [OperationalError('near "*": syntax error',)]
```

## Log
```python
m = Medoo(
	database_type = "sqlite",
	database_file = ":memory:",
 
	# Enable logging
	logging =  True
)
 
m.select("account", None, [
	"user_name",
	"email"
], {
	"user_id[<]":  20
})
 
m.insert("account", None, {
	"user_name":  "foo",
	"email":  "foo@bar.com"
})
 
print m.log()
# [
#	"SELECT "user_name","email" FROM "account" WHERE "user_id" < 20",
#	"INSERT INTO "account" ("user_name", "email") VALUES ('foo', 'foo@bar.com')"
# ]
 
# Will output only one last record if "logging":  false or ignored by default on initialization
# [
#	"INSERT INTO "account" ("user_name", "email") VALUES ('foo', 'foo@bar.com')"
# ]
```

## Last
```python
m.select("account", None, [
	"user_name",
	"email"
], {
	"user_id[<]":  20
})
 
m.insert("account", {
	"user_name":  "foo",
	"email":  "foo@bar.com"
})
 
print m.last()
# INSERT INTO "account" ("user_name", "email") VALUES ('foo', 'foo@bar.com')
```

[1]: https://medoo.in/
[2]: https://github.com/kayak/pypika
