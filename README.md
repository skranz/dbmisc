## Overview

Author: Sebastian Kranz, Ulm University

The package `dbmisc` contains some helper functions to work with databases. Core idea is that you specify a schema of your database as a simple YAML file (see example in the folder inst/examples/dbschema).

Having a schema file you can create or update database tables using the function `dbCreateSchemaTables` or create a complete new SQLite database with the function `dbCreateSQLiteFromSchema`.

The function `set.db.schemas` assigns a schema to a database connection. The functions `dbGet`, `dbInsert`, `dbUpdate` and `dbDelete` then allow common database operations making sure that types a properly converted between R and the database (so far only tested for SQLite) based on the provided schema.

These functions also have an argument `log.dir` that allows to create a simple log of all modifications of the database.

The function `dbGetMemoise` buffers database results in memory. If you call it again with the same parameter and the log file suggests no changes inbetween to the database, you will get the results from memory. This may be useful in multi use apps, where you want to make sure that each user has an up to date version of the data, but want to avoid unneccessary reloads if nothing has changed inbetween.

## Installation

`dbmisc` is hosted on Github only. To install it, run the following lines of code:

```r
if (!require(devtools)) install.packages("devtools")

devtools::install_github("skranz/restorepoint")
devtools::install_github("skranz/stringtools")
devtools::install_github("skranz/dbmisc")
```


