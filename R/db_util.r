
#' Insert row into table
#'
#' @param conn dbi database connection
#' @param table name of the table
#' @param vals named list of values to be inserted
#' @param sql optional a parameterized sql string
#' @param run if FALSE only return parametrized SQL string
#' @param mode "insert" or "replace", should have no effect so far
dbInsert = function(conn, table, vals,schema=NULL, sql=NULL,run=TRUE, mode=c("insert","replace")[1], rclass=schema$rclass, convert=!is.null(rclass), primary.key = schema$primary_key, get.key=FALSE) {
  restore.point("dbInsert")
  cols = names(vals)

  if (isTRUE(convert)) {
    names = names(rclass)
    vals = suppressWarnings(lapply(names, function(name) {
      as(vals[[name]],rclass[[name]])
    }))
    names(vals) = names
  }

  if (is.null(sql)) {
    sql <- paste0(mode, " into ", table," values (",
      paste0(":",cols,collapse=", "),")")
  }
  if (!run) return(sql)

  if (length(vals[[1]])>1) {
    vals = as.data.frame(vals,stringsAsFactors = FALSE)
    dbWriteTable(conn, table, value=vals, append=TRUE)
  } else {
    ret = dbSendQuery(conn, sql, params=vals)
  }

  if (!is.null(primary.key) & get.key) {
    rs = dbSendQuery(conn, "select last_insert_rowid()")
    pk = dbFetch(rs)
    vals[[primary.key]] = pk[,1]
  }


  invisible(list(values=vals))
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

#' Load and init database table schemas from yaml file
#'
#' @param file file name
#' @param yaml yaml as text
load.and.init.schemas = function(file=NULL, yaml=NULL) {
  if (is.null(file)) {
    schemas = yaml.load(paste0(yaml, collapse="\n"))
  } else {
    schemas = yaml.load_file(file)
  }
  names = names(schemas)
  schemas = lapply(names, function(name) {
    init.schema(schemas[[name]],name=name)
  })
  names(schemas) = names
  schemas
}

#' Init a schema by parsing table definition and store info
#' in easy accessibale R format
#'
#' Create rclasses of each column and primary keys
#'
#' @param schema the table schema as an R list
#' @param name of the table
init.schema = function(schema, name=NULL) {
  schema$rclass = schema.r.classes(schema)
  schema$name = name

  cols = sapply(schema$table, tolower)
  rows = grep("integer primary key", cols,fixed = TRUE)
  if (length(rows)>0)
    schema$primary_key = names(schema$table)[rows[1]]
  schema
}

schema.r.classes = function(schema) {
  str = tolower(substring(schema$table,1,3))

  classes =c(
    cha = "character",
    tex = "character",
    var = "character",
    boo = "logical",
    int = "integer",
    num = "numeric",
    rea = "numeric",
    dat = "datetime"
  )
  res = classes[str]
  names(res) = names(schema$table)
  res
}

example.empty.row.schema = function() {
  setwd("D:/libraries/dbmisc/dbmisc/inst/examples/dbschema")
  schemas = yaml.load_file("strattourndb.yaml")
  row = empty.row.from.schema(schemas$userstrats)
  lapply(row, class)
  list.to.schema.template(row)
}

schema.template = function(li, name="mytable", toClipboard=TRUE) {
  templ = c(
    "character" = "VARCHAR(255)",
    "integer" = "INTEGER",
    "numeric" = "NUMERIC",
    "logical" = "BOOLEAN",
    "POSIXct" = "DATETIME"
  )

  is.subli = sapply(li, function(el) is.list(el))

  eli = li[!is.subli]
  cols = lapply(eli, function(el) {
    cl = class(el)[[1]]
    if (cl %in% names(templ)) {
      return(templ[cl])
    }
    return("UNKNOWN")
  })
  cols = paste0("    ",names(eli),": ", cols)
  txt = paste0('
',name,':
  descr:
  table:
',paste0(cols,collapse='\n'),'
  indexes:
')

  stxt = sapply(names(li)[is.subli], function(name) {
    list.to.schema.template(li[[name]],name, toClipboard=FALSE)
  })
  txt = paste0(c(txt,stxt), collapse="\n")
  if (toClipboard) {
    writeClipboard(txt)
  }
  cat(txt)
  invisible(txt)
}

#' Creates an example row from a database schema table
#' using provided column values and default values specified in schema
empty.row.from.schema = function(.schema, ..., .use.defaults = TRUE) {
  restore.point("schema.value.list")

  empty = list(
    "logical" = NA,
    "numeric" = NA_real_,
    "character" = '',
    "integer" = NA_integer_,
    "datetime" = NA_real_
  )
  if (is.null(.schema$rclass)) {
    classes = schema.r.classes(.schema)
  } else {
    classes = .schema$rclass
  }

  vals = empty[classes]
  table = .schema$table
  names(vals) = names(table)
  if (.use.defaults & !is.null(.schema$defaults)) {
    vals[names(.schema$defaults)] = .schema$defaults
  }
  args = list(...)
  vals[names(args)] = args
  vals
}

#' Creates an example data frame from a database schema table
#' using provided column values and default values specified in schema
empty.df.from.schema = function(.schema,.nrows=1, ..., .use.defaults = TRUE) {
  restore.point("empty.df.from.schema")
  li = empty.row.from.schema(.schema, ..., .use.defaults=.use.defaults)
  if (.nrows==1) return(as.data.frame(li))

  df = as.data.frame(lapply(li, function(col) rep(col,length.out = .nrows)))
  df
}
