
#' Insert row into table
#'
#' @param conn dbi database connection
#' @param table name of the table
#' @param vals named list of values to be inserted
#' @param sql optional a parameterized sql string
#' @param run if FALSE only return parametrized SQL string
#' @param mode "insert" or "replace", should have no effect so far
dbInsert = function(conn, table, vals,sql=NULL,run=TRUE, mode=c("insert","replace")[1]) {
  restore.point("dbInsert")
  cols = names(vals)

  if (is.null(sql)) {
    sql <- paste0(mode, " into ", table," values (",
      paste0(":",cols,collapse=", "),")")
  }
  if (!run) return(sql)
  ret = dbSendQuery(conn, sql, params=vals)
  invisible(ret)
}


#' Delete row(s) from table
#'
#' @param conn dbi database connection
#' @param table name of the table
#' @param params named list of values for key fields that identify the rows to be deleted
#' @param sql optional a parameterized sql string
#' @param run if FALSE only return parametrized SQL string
dbDelete = function(conn, table, params, sql=NULL, run = TRUE) {
  restore.point("dbDelete")
  if (is.null(sql)) {
    if (length(params)==0) {
      where = ""
    } else {
      where = paste0(" where ", paste0(names(params)," = :",names(params), collapse= " AND "))
    }
    sql = paste0('delete from ', table, where)
  }
  if (!run)
    return(sql)
  rs = dbSendQuery(conn, sql, params=params)
  rs
}

#' Get rows from a table
#'
#' @param .db dbi database connection
#' @param .table name of the table
#' @param .par named list of values for key fields that identify the rows to be deleted
#' @param ... alternatively parameters can be provided directly as named arguments
#' @param .sql optional a parameterized sql string
#'        if you want to insert into the sql string
#'        the value from a provided parameter mypar
#'        write :mypar in the SQL string at the corresponding position.
#'        Example:
#'
#'        select * from mytable where name = :myname
#'
#' @param .run if FALSE only return parametrized SQL string
dbGet = function(.db, .table=NULL,..., .par=NULL, .sql=NULL, .run = TRUE) {
  .par = c(.par,list(...))
  restore.point("dbGet")
  if (is.null(.sql)) {
    if (tolower(substring(.table,1,7))=="select ") {
      .sql = .table
    } else {
      if (length(.par)==0) {
        where = ""
      } else {
        where = paste0(" where ", paste0(names(.par)," = :",names(.par), collapse= " AND "))
      }
      .sql = paste0('select * from ', .table, where)
    }
  }
  if (!.run) return(.sql)
  rs = dbSendQuery(.db, .sql, params=.par)
  res = dbFetch(rs)
  if (NROW(res)==0) return(NULL)
  res
}

#' Create database tables and possible indices from a simple yaml schema
#'
#' @param conn dbi database connection
#' @param schema a schema as R list
#' @param schema.yaml alternatively a schema as yaml text
#' @param schema.file alternatively a file name of a schema yaml file
#' @param overwrite shall existing tables be overwritten?
#' @param silent if TRUE don't show messages
dbCreateSchemaTables = function(conn,schema=NULL, schema.yaml=NULL, schema.file=NULL, overwrite=FALSE,silent=FALSE) {
  restore.point("dbCreateSchemaTables")

  if (is.null(schema)) {
    if (is.null(schema.yaml))
      schema.yaml = readLines(schema.file,warn = FALSE)
    schema.yaml = paste0(schema.yaml, collapse = "\n")
    schema = yaml.load(schema.yaml)
  }

  tables = names(schema)
  lapply(tables, function(table) {
    s = schema[[table]]
    if (overwrite)
      try(dbRemoveTable(conn, table), silent=silent)
    if (!dbExistsTable(conn, table)) {
      # create table
      sql = paste0("CREATE TABLE ", table,"(",
        paste0(names(s$table), " ", s$table, collapse=",\n"),
        ")"
      )
      dbSendQuery(conn,sql)

      # create indexes
      for (index in s$indexes) {
        err = try(dbSendQuery(conn,index), silent=TRUE)
        if (is(err,"try-error")) {
          msg = as.character(err)
          msg = str.right.of(msg,"Error :")
          msg = paste0("When running \n", index,"\n:\n",msg)
          stop(msg)
        }
      }
    }
  })
  invisible(schema)
}
