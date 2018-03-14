## Brief Overview

Author: Sebastian Kranz, Ulm University

The package `dbmisc` contains some helper functions to work with databases. Core idea is that you specify a schema of your database as a simple YAML file (see example below).

Having a schema file you can create or update database tables using the function `dbCreateSchemaTables` or create a complete new SQLite database with the function `dbCreateSQLiteFromSchema`.

The function `set.db.schemas` assigns a schema to a database connection. The functions `dbGet`, `dbInsert`, `dbUpdate` and `dbDelete` then allow common database operations making sure that types a properly converted between R and the database (so far only tested for SQLite) based on the provided schema.

## Installation

`dbmisc` is hosted on Github only. To install it, run the following lines of code:

```r
if (!require(devtools)) install.packages("devtools")

devtools::install_github("skranz/restorepoint")
devtools::install_github("skranz/stringtools")
devtools::install_github("skranz/dbmisc")
```

## Starting Guide

### Schema file and creation of database tables
The example schema [https://github.com/skranz/dbmisc/blob/master/inst/examples/dbschema/userdb.yaml](userdb.yaml) specifies a database with just one table `user` where you might want to store some user data.

```yaml
user:
  table:
    userid: CHARACTER(20)
    email: VARCHAR(100)
    age: INTEGER
    female: BOOLEAN
    created: DATETIME
    descr: TEXT
  index:
    - email
    - [female, age]
    - created
  sql:
    - "CREATE UNIQUE INDEX index_userid ON user (userid)"
```
Under the field `table`, all columns of the table are specified using the variable types of the database. 
The field `index` specifies three indices on the table. The second index `[female, age]` is an index on two columns. 
The field `sql` allows custom SQL commands that will be run when the table is generated. Here we want to generate a special `unique` index on the column `userid`, which requires the manual SQL code.

The following R code generates a new SQLite database from this schema in your current working directory:
```r
schema.file = system.file("examples/dbschema/userdb.yaml", package="dbmisc")
db.dir = getwd()
dbCreateSQLiteFromSchema(schema.file=schema.file, db.dir=db.dir, db.name="userdb.sqlite")
```
You can take a look at the generated database with some software like [https://sqlitestudio.pl](https://sqlitestudio.pl).

Alternatively, if you already have some connection `db` to an existing database, you can create or update existing tables based on your schema file with the command `dbCreateSchemaTable`. Example:

```r
db = dbConnect(RSQLite::SQLite(), file.path(db.dir, "userdb.sqlite"))
dbCreateSchemaTables(db=db,schema.file=schema.file,update=TRUE)
```
The argument `update=TRUE` (TRUE is the default value for update), means that if a table with a same name as in the schema already exists, the table is updated given the new schema. The existing data is converted to the new schema. Newly added columns will be filled with NA values. If `update=FALSE` all existing data is deleted.


It could be the case that you already have some data in R, e.g. from a CSV file, which you want to store in a database table. To avoid typing the whole schema, you can use the little helper function `schema.template`, which generates a skeleton of the yaml code for the schema and copies it to the clipboard. Consider the following code
```r
df = data.frame(a=1:5,b="hi",c=Sys.Date(),d=Sys.time())
schema.template(df,"mytable")
```
It copies to your clipboard the following yaml output, which you can the manually adapt
```yaml
mytable:
  table:
    a: INTEGER
    b: VARCHAR(255)
    c: DATE
    d: DATETIME
  index:
    - a # example index on first column
```

### Database operations with a schema file

The functions `dbGet`, `dbInsert`, `dbUpdate` and `dbDelete` allow common database operations that can use a schema file to facilitate type conversion between R and the database. (So far only tested for SQLite).

The easiest way to use a schema file, is to assign it to a database connection with the command `set.db.schema`
```r
db = dbConnect(RSQLite::SQLite(), file.path(db.dir, "userdb.sqlite"))
db = set.db.schema(db, schema.file=schema.file)
```

The following example inserts an entry into our table `user`:
```r
user = list(created=Sys.time(), userid="user1",age=47, female=TRUE, email="test@email.com", gender="female")
dbInsert(db,table="user", user)
```

Recall that the table `user` has been specified with the following columns
```
userid: CHARACTER(20)
email: VARCHAR(100)
age: INTEGER
female: BOOLEAN
created: DATETIME
descr: TEXT
```
Our R list differs in certain aspects from the table: i) the order of fields is not the same as in the database table, ii) we have not specified the column `descr`, iii) we have an additional value `gender` that is not part of the database. Using the schema, the function `dbInsert` conveniently corrects for these differences, that is it removes `gender`, sets an `NA` value for `descr` and orders the values in the right order.

This allows to avoid some boilerplate code when performing database operations. Of course, whether one likes such autocorrections is a matter of taste: I find it quite convenient, but it may also make it harder to catch some errors. 

The function `dbInsert` also performs some data type conversions that so far are not automatically performed by the functions in the `DBI` interfaces, e.g. the `POSIXct` variable `created` to a format, in which we can store and retrieve DATETIME values from the database.

You can also pass a data.frame to `dbInsert` in order to insert multiple rows at once.


### To be continued...


These functions also have an argument `log.dir` that allows to create a simple log of all modifications of the database.

The function `dbGetMemoise` buffers database results in memory. If you call it again with the same parameter and the log file suggests no changes inbetween to the database, you will get the results from memory. This may be useful in multi use apps, where you want to make sure that each user has an up to date version of the data, but want to avoid unneccessary reloads if nothing has changed inbetween.

