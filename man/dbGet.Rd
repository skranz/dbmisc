% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/db_util.r
\name{dbGet}
\alias{dbGet}
\title{Get rows from a table}
\usage{
dbGet(
  db,
  table = NULL,
  params = NULL,
  sql = NULL,
  fields = NULL,
  joinby = NULL,
  jointype = c("inner", "left", "right")[1],
  run = TRUE,
  schema = if (length(table) == 1) schemas[[table]] else NULL,
  schemas = get.db.schemas(db),
  rclass = schema$rclass,
  convert = !is.null(rclass),
  convert.param = FALSE,
  orderby = NULL,
  null.as.na = TRUE,
  origin = "1970-01-01",
  where.in = FALSE,
  where.sql = NULL,
  empty.as.null = FALSE,
  n = -1
)
}
\arguments{
\item{db}{dbi database connection}

\item{table}{name of the table. If you specify more than one table the later tables will be joined. You then should specify the \code{joinby} argument and possible the \code{fields} argument if you want to select fields also from the later tables.}

\item{params}{named list of values for key fields. If you don't use a custom SQL statement the list will be used to construct a WHERE clause. E.g. \code{params = list(age=30,gender="male")} would be translated to the WHERE clause \code{WHERE age = 30 AND gender="male"}. If you want to match several values, e.g. \code{params = list(age = c(30,40))} you need to set the argument \code{where.in = TRUE} to construct a correct WHERE clause.}

\item{sql}{optional a parameterized custom sql string
  Can contain parameters passed with the \code{param} arguments.
  E.g. if you have \code{param = list(myname="Seb")} you could use \code{myname} in an SQL statement as follows:

   select * from mytable where name = :myname

  To avoid SQL injection you should provide all values that
  can be provided by a user as such parameters or
  make sure that you escape them.}

\item{fields}{If not NULL can be used to specify fields that shall be selected as character. For joined tables, you must enter fields in the format "tablename.field". E.g. \code{fields = "*, table2.myfield"} would select all columns from the first table and the column \code{myfield} from the joined 2nd table.}

\item{joinby}{If you specify more than one table the later tables shall be joined by the variables specified in \code{joinby} with the first table. For more complicated joins where the names of the join variables differ you have to write custom SQL with the \code{sql} argument instead.}

\item{jointype}{The type of the join if you specify a \code{joinby} argument. Default is "inner" but can also be set to "left" or "right"}

\item{run}{if FALSE only return parametrized SQL string}

\item{schema}{a table schema that can be used to convert values}

\item{rclass}{the r class of the table columns, is extracted from schema}

\item{convert}{if rclass is given shall results automatically be converted to these classes?}

\item{orderby}{names of columns the results shall be ordered by as character vector. Add "DESC" or "ASC" after column name to sort descending or ascending. Example: \code{orderby = c("pos DESC","hp ASC")}}

\item{null.as.na}{shall NULL values be converted to NA values?}

\item{origin}{the origin date for DATE and DATETIME conversion}

\item{where.in}{Set TRUE if your params contain sets and therefore a WHERE IN clause shall be generated.}

\item{where.sql}{An optional SQL code just for the WHERE clause. Can be used if some parameters will be checked with inequality.}

\item{empty.as.null}{if TRUE return just NULL if the query returns zero rows.}

\item{n}{The maximum number of rows that shall be fetched. If \code{n=-1} (DEFAULT) fetch all rows.}
}
\description{
Get rows from a table
}
