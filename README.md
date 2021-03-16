## The dbmisc package for using SQLite more conveniently in R

Author: Sebastian Kranz, Ulm University

The package `dbmisc` contains some helper functions to work with databases, e.g. the functions `dbGet`, `dbInsert`, `dbUpdate` and `dbDelete` simplify common database operations. One main motivation for `dbmisc` is to facilitate automatic type conversion between R and the database.

For this purpose you can specify a schema of your database as a simple YAML file. With the schema file you can also easily create or update database tables.

There is further functionality that allows automatic logs of database operations and memoization of fetched values. Basic usage is explaned in this README file.

## Installation

`dbmisc` is hosted on my own drat-powered R archive. To install it, run the following code:


```r
install.packages("dbmisc",
  repos = c("https://skranz-repo.github.io/drat/",getOption("repos")))
```

### Schema file and creation of database tables

The example schema [userdb.yaml](https://github.com/skranz/dbmisc/blob/master/inst/examples/dbschema/userdb.yaml) specifies a database with just one table `user` ([here](https://github.com/skranz/dbmisc/blob/master/inst/examples/dbschema/coursedb.yaml) is an example with more than one table):

```yaml
user:
  table:
    userid: TEXT
    email: TEXT
    age: INTEGER
    female: BOOLEAN
    created: DATETIME
    descr: TEXT
  unique_index:
    - userid
  index:
    - email
    - [female, age]
    - created
```

Under the field `table`, all columns of the table are specified using the variable types of the database.
The field `unique_index` specifies that `userid` is a unique index column, i.e. no two rows with duplicated `userid` can exist in the database.

The field `index` specifies three non-unique indices on the table. The second index `[female, age]` is an index on two columns.

The following R code generates a new SQLite database from this schema in your current working directory:

```r
schema.file = system.file("examples/dbschema/userdb.yaml", package="dbmisc")
db.dir = getwd()
dbCreateSQLiteFromSchema(schema.file=schema.file, db.dir=db.dir, db.name="userdb.sqlite")
```
You can take a look at the generated database with some software like [https://sqlitestudio.pl](https://sqlitestudio.pl).

If you want to update the schema for an existing data base you, can use the argument `update=TRUE`:

```r
dbCreateSQLiteFromSchema(db=db,schema.file=schema.file,update=TRUE)
```
Then the existing data is converted to the new schema. Newly added columns will be filled with NA values. If `update=FALSE`, all existing data is deleted.

Of course, for safety reasons always make a backup of your database, before you modify it in this way.

It could be the case that you already have some data in R, e.g. from a CSV file, for which you want to generate a database table. To avoid typing the whole schema, you can use the little helper function `schema.template`, which generates a skeleton of the yaml code for the schema and copies it to the clipboard. Consider the following code


```r
df = data.frame(a=1:5,b="hi",c=Sys.Date(),d=Sys.time())
schema.template(df,"mytable")
```

It copies to your clipboard the following yaml output, which you can the manually adapt

```yaml
mytable:
  table:
    a: INTEGER
    b: TEXT
    c: DATE
    d: DATETIME
  index:
    - a # example index on first column
```

### Opening a data base connection with a schema

The functions `dbGet`, `dbInsert`, `dbUpdate` and `dbDelete` allow common database operations that can use a schema file to facilitate type conversion between R and the database. (So far only tested for SQLite).

The easiest way to use a schema file, is to open the connection and assign the schema with a single command:


```r
db = dbConnectSQLiteWithSchema("userdb.sqlite", schema.file)
```

Alternatively, you could also assign a schema to an existing database connection with the command `set.db.schema`

```r
db = set.db.schema(db, schema.file=schema.file)
```

I typically write for my shiny apps a little function to get the database connection, like:

```r
get.userdb = function(db.dir=getwd()) {
  db = getOption("userdb.connection")
  if (is.null(db)) {
    schema.file = system.file("schema/userdb.yaml",package = "shinyUserDB")
    db.file = file.path(db.dir, "userdb.sqlite") 
    db = dbConnectSQLiteWithSchema(db.file,schema.file)
    options(userdb.connection=db)
  }
  db
}
```

The function opens just a single database connection that is used by all app instances and stores it as a global option. If the connection is already open and the function is called again, the connection is retrieved from a global option. I am not sure whether there is any benefit from [pooling](https://db.rstudio.com/pool/) more than one connection with SQLite.

### Inserting data

The following example inserts an entry into our table `user`:


```r
new_user = list(created=Sys.time(), userid="user1",age=47, female=TRUE, email="test@email.com", gender="female")
dbInsert(db,table="user", new_user)
```

Recall that the table `user` has been specified with the following columns
```
userid: TEXT
email: TEXT
age: INTEGER
female: BOOLEAN
created: DATETIME
descr: TEXT
```

Our R list differs in certain aspects from the table: 

  i) the order of fields is not the same as in the database table, 
  
  ii) we have not specified the column `descr`
  
  iii) we have an additional value `gender` that is not part of the database.
  
Using the schema, the function `dbInsert` conveniently corrects for these differences: 

  i) orders the values in the right order, 
  
  ii) it adds a value `descr`filled set to NA, and 
  
  iii) it removes `gender`.

This 'autocorrection' allows to avoid some boilerplate code when performing database operations.

The function `dbInsert` also performs some data type conversions that seem not automatically performed by the functions in the `DBI` interfaces, e.g. it sets the `POSIXct` variable `created` to a DATETIME format that can be stored retrieved from the SQLite database (as SQLite does not really have a DATETIME format it is stored as a floating point number).

You can also pass a data frame to `dbInsert` in order to insert multiple rows at once.

If you set the argument `run=FALSE`, `dbInsert` performs no data base action but just returns the SQL statement that would be run: 

```r
dbInsert(db,table="user", new_user,run = FALSE)
```

```
## [1] "insert into user values (:userid, :email, :age, :female, :created, :descr)"
```
Note that `dbmisc` prepares by default [parametrized queries](https://db.rstudio.com/best-practices/run-queries-safely/#parameterized-queries) to avoid [SQL-injections](https://db.rstudio.com/best-practices/run-queries-safely).


### Getting data with dbGet

The function `dbGet` retrieves data from the database. E.g. the command


```r
dat = dbGet(db,table="user", list(userid="user1"))
```

returns a data frame with one row in which `userid` is equal to "user1". Again types are converted to standard R formats. For example, SQLite stores BOOLEANS internally as INTEGER, but based on the schema, `dbGet` will correctly convert the variable `female` to a `logical` variable in R. Also DATETIME variables will be correctly converted to `POSIXct`.

If you set the argument `run=FALSE`, you can get the SQL query that `dbGet` runs:


```r
dbGet(db,table="user", list(userid="user1"), run = FALSE)
```

```
## [1] "SELECT * FROM user WHERE userid = :userid"
```


The `dbGet` command allows also for more flexible queries. For example, one can specify selective fields with the argument `fields`. One can also specify multiple tables joint by the columns specified in `joinby`:


```r
dbGet(db,c("course","coursestud"), list(courseid="course1"),
      fields = "*, coursestud.email", joinby=c("courseid"), run = FALSE)
```

```
## [1] "SELECT *, coursestud.email FROM course INNER JOIN coursestud USING(courseid) WHERE courseid = :courseid"
```

You can also provide a custom SQL command as the argument `sql`:


```r
sql = "SELECT *, coursestud.email FROM course INNER JOIN coursestud USING(courseid) WHERE courseid = :courseid"

dbGet(db,table=c("course","coursestud"),list(courseid="course1"),sql=sql)
```

Even for a custom SQL command you can provide one or multiple tables to use the associated schemas when converting data types and parameters that will be used in the parametrized query. 

### dbUpdate and dbDelete

The function [dbUpdate](https://skranz.github.io/dbmisc/reference/dbUpdate.html) and [dbDelete](https://skranz.github.io/dbmisc/reference/dbDelete.html) are similar helper functions to update or delete data sets.

### Logging database modifications

The functions `dbInsert`, `dbUpdate` and `dbDelete` also have an argument `log.dir`. If provided each call adds an entry to a simple log file that allows to check when some modifications of the database took place.

### Memoisation

The following functionality was useful for some of my shiny apps.
The function `dbGetMemoise` buffers database results in memory. By default, if you call the function again with the same parameters it will get the results from memory.

There is also an argument `refetch.if.changed` which is by default TRUE if and only if you provide a non-null argument `log.dir`. When set to TRUE `dbGetMemoise` will load the data again from the database if the log file has changed.


