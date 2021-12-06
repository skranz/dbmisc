.dbmisc.memoise.env = new.env()

#' Creates a connection to an SQLite database and sets
#' the specified schema
#'
#' The schema is added as attribute to the connection
#' object and is automatically used by \code{dbInsert},
#' \code{dbGet}, and \code{dbUpdate}.
#'
#' @param dbname Filename of the SQLite database
#' @param schema.file YAML file that contains the database schema. By default it is assumed to be in the same folder as the database with the same name but the extension ".yaml"
#' @param schema If you already loaded a schema file manually with \code{load.and.init.schemas}, you can also provide it here instead of specifying a schema.file.
dbConnectSQLiteWithSchema = function(dbname, schema.file=paste0(tools::file_path_sans_ext(dbname),".yaml"), schema=load.and.init.schemas(schema.file)) {
  db = dbConnect(RSQLite::SQLite(),dbname)
  db = set.db.schemas(db, schema)
  db
}

#' Set schemas as hidden attribute to a data base connection db
set.db.schema = set.db.schemas = function(db, schemas=NULL, schema.file=NULL) {
  if (is.null(schemas) & !is.null(schema.file)) {
    schemas = load.and.init.schemas(file=schema.file)
  }

  attr(db,"schemas") = schemas
  invisible(db)
}

#' Extract schemas from a data base connection
get.db.schema = get.db.schemas = function(db, warn.null=TRUE) {
  schemas = attr(db,"schemas")
  if (is.null(schemas) & warn.null) {
    attr(db,"no.schema.warning",TRUE)
    if (!isTRUE(attr(db,"no.schema.warning"))) {
      cat("\nno schemas set for db connection")
    }
  }
  schemas
}


get.table.memoise = function(hash) {
  .dbmisc.memoise.env[[hash]]
}

set.table.memoise = function(hash, data) {
  .dbmisc.memoise.env[[hash]] = list(time=Sys.time(),data=data)
}


#' log a command that changes a database
logDBcommand = function(type, sql="", user="NA", log.dir=NULL, table=NULL, do.log = TRUE) {
  restore.point("logDBcommand")

  if (!do.log | is.null(log.dir)) return()

  if (!is.null(table)) {
    log.file = paste0(log.dir,"/",table,".log")
  } else {
    log.file = paste0(log.dir,"/__sql__.log")
  }
  txt = paste0(list(as.character(Sys.time()), user, type,sql), collapse="|")

  try(write(txt, file=log.file,append=TRUE))
}


#' Insert row(s) into table
#'
#' @param db dbi database connection
#' @param table name of the table
#' @param vals named list of values to be inserted
#' @param schema a table schema that can be used to convert values
#' @param sql optional a parameterized sql string
#' @param run if FALSE only return parametrized SQL string
#' @param mode "insert" or "replace", should have no effect so far
#' @param add.missing.cols if TRUE (default) and a schema is provided than automatically add database columns that are missing in \code{vals} and set them NA.
#' @param rclass the r class of the table columns, is extracted from schema
#' @param convert if rclass is given shall results automatically be converted to these classes?
#' @param primary.key name of the primary key column (if the table has one)
#' @param get.key if TRUE return the created primary key value
dbInsert = function(db, table=NULL, vals,schema=schemas[[table]], schemas=get.db.schemas(db), sql=NULL,run=TRUE, mode=c("insert","replace")[1], add.missing.cols=TRUE, rclass=schema$rclass, convert=!is.null(rclass), primary.key = schema$primary_key, get.key=FALSE, null.as.na=TRUE, log.dir=NULL, do.log=!is.null(log.dir), user=NA) {
  restore.point("dbInsert")

  # Nothing to insert
  if (NROW(vals)==0) return()

  # Update vals based on table schema
  if (isTRUE(convert)) {
    vals = convert.r.to.db(vals=vals,rclass = rclass,schema = schema,null.as.na = null.as.na,add.missing = add.missing.cols)
  }
  cols = names(vals)

  if (is.null(sql)) {
    sql <- paste0(mode, " into ", table," values (",
      paste0(":",cols,collapse=", "),")")
  }
  if (!run) return(sql)

  if (length(vals[[1]])>1) {
    vals = as.data.frame(vals,stringsAsFactors = FALSE)
    dbWriteTable(db, table, value=vals, append=TRUE)
  } else {
    #ret = dbSendQuery(db, sql, params=vals)
    ret = try(dbSendQuery(db, sql, params=vals))
    if (is(ret, "try-error")) {
      msg = as.character(ret)
      if (has.substr(msg,"were supplied")) {
        db.cols = dbTableCols(db, table)$name
        missing = setdiff(db.cols,names(vals))
        if (length(missing)>0) {
          msg = paste0(msg, "\nCompared to your database the following columns are missing in your provided data set:\n\n", paste0(missing, collapse=", "))
          if (is.null(schema$table) & add.missing.cols) {
            msg = "\nYou did not specify a schema so that missing columns were not automatically filld with NA."
          } else if (add.missing.cols) {
            msg = paste0(msg, "\n\nLikely, you get this message because your DB is not synchronized with your current schema. Make sure to restart R and then call dbmisc::dbCreateSQLiteFromSchema(update=TRUE, ...).")
          }
        }
      }
      stop(msg)
    }


  }

  if (!is.null(primary.key) & get.key) {
    rs = dbSendQuery(db, "select last_insert_rowid()")
    pk = dbFetch(rs)
    vals[[primary.key]] = pk[,1]
  }

  logDBcommand(user = user,type=mode,sql=sql,log.dir=log.dir, do.log=do.log,table = table)


  invisible(list(values=vals))
}

#' Delete row(s) from table
#'
#' @param db dbi database connection
#' @param table name of the table
#' @param params named list of values for key fields that identify the rows to be deleted
#' @param sql optional a parameterized sql string
#' @param run if FALSE only return parametrized SQL string
dbDelete = function(db, table, params, sql=NULL, run = TRUE, log.dir=NULL, do.log=!is.null(log.dir), user=NA, where.in=FALSE) {
  restore.point("dbDelete")
  if (is.null(sql)) {
    if (length(params)==0) {
      where = ""
    } else {
      where = sql.where.code(db, params, where.in=where.in)
    }
    sql = paste0('delete from ', table, where)
  }
  if (!run)
    return(sql)
  if (length(params)>0) {
    rs = dbSendQuery(db, sql, params=params)
  } else {
    rs = dbSendQuery(db, sql)
  }

  logDBcommand(user = user,type="mode",sql=sql,log.dir=log.dir, do.log=do.log,table = table)
  rs
}

#' Get results from a database like dbGet put buffer the results in memory
#'
#' If the function is called again with the same parameter check if the
#' something was changed in the database inbetween by looking at the time
#' stamp of the log file. If there were no changes restore the values from
#' memory. If there were changes load data again from database.
#'
#' If refetch.if.changed = FALSE (default if no log.dir is provided), always
#' use the data from memory.
#'
dbGetMemoise = function(db, table,params=NULL,schema=schemas[[table]], schemas=get.db.schemas(db), log.dir=NULL, refetch.if.changed = !is.null(log.dir), empty.as.null=FALSE,...) {
  restore.point("dbGetMemoise")
  library(digest)
  hash = digest::digest(list(table, params))
  res = get.table.memoise(hash)
  use.memoise = !is.null(res)
  if (use.memoise) {
    if (refetch.if.changed) {
      file = paste0(log.dir,"/",table,".log")
      mtime = file.mtime(file)
      use.memoise = res$time > mtime
    }
  }
  if (use.memoise) return(res$data)

  # fetch data and store result
  data = dbGet(db=db, table=table,params=params, schema=schema,empty.as.null=empty.as.null,...)
  set.table.memoise(hash = hash, data=data)
  data
}


#' Get rows from a table
#'
#' @param db dbi database connection
#' @param table name of the table. If you specify more than one table the later tables will be joined. You then should specify the \code{joinby} argument and possible the \code{fields} argument if you want to select fields also from the later tables.
#' @param params named list of values for key fields. If you don't use a custom SQL statement the list will be used to construct a WHERE clause. E.g. \code{params = list(age=30,gender="male")} would be translated to the WHERE clause \code{WHERE age = 30 AND gender="male"}. If you want to match several values, e.g. \code{params = list(age = c(30,40))} you need to set the argument \code{where.in = TRUE} to construct a correct WHERE clause.
#' @param sql optional a parameterized custom sql string
#'   Can contain parameters passed with the \code{param} arguments.
#'   E.g. if you have \code{param = list(myname="Seb")} you could use \code{myname} in an SQL statement as follows:
#'
#'    select * from mytable where name = :myname
#'
#'   To avoid SQL injection you should provide all values that
#'   can be provided by a user as such parameters or
#'   make sure that you escape them.
#' @param fields If not NULL can be used to specify fields that shall be selected as character. For joined tables, you must enter fields in the format "tablename.field". E.g. \code{fields = "*, table2.myfield"} would select all columns from the first table and the column \code{myfield} from the joined 2nd table.
#' @param joinby If you specify more than one table the later tables shall be joined by the variables specified in \code{joinby} with the first table. For more complicated joins where the names of the join variables differ you have to write custom SQL with the \code{sql} argument instead.
#' @param jointype The type of the join if you specify a \code{joinby} argument. Default is "inner" but can also be set to "left" or "right"
#' @param run if FALSE only return parametrized SQL string
#' @param schema a table schema that can be used to convert values
#' @param rclass the r class of the table columns, is extracted from schema
#' @param convert if rclass is given shall results automatically be converted to these classes?
#' @param orderby names of columns the results shall be ordered by as character vector. Add "DESC" or "ASC" after column name to sort descending or ascending. Example: \code{orderby = c("pos DESC","hp ASC")}
#' @param where.in Set TRUE if your params contain sets and therefore a WHERE IN clause shall be generated.
#' @param where.sql An optional SQL code just for the WHERE clause. Can be used if some parameters will be checked with inequality.
#' @param null.as.na shall NULL values be converted to NA values?
#' @param origin the origin date for DATE and DATETIME conversion
#' @param empty.as.null if TRUE return just NULL if the query returns zero rows.
#' @param n The maximum number of rows that shall be fetched. If \code{n=-1} (DEFAULT) fetch all rows.
dbGet = function(db, table=NULL,params=NULL, sql=NULL, fields=NULL, joinby = NULL, jointype=c("inner","left","right")[1], run = TRUE, schema= if(length(table)==1) schemas[[table]] else NULL, schemas=get.db.schemas(db), rclass=schema$rclass, convert = !is.null(rclass), convert.param=FALSE, orderby=NULL, null.as.na=TRUE, origin = "1970-01-01", where.in=FALSE, where.sql = NULL, empty.as.null=FALSE, n=-1) {


  restore.point("dbGet")

  # We may use schemas from multiple tables in complex
  # sql statements
  if (is.null(schema) & length(table)>1 & !is.null(schemas) & run) {
    schema = list(rclass = unlist(lapply(schemas[table], function(s) s$rclass)))
    names(schema$rclass) = str.right.of(names(schema$rclass),".")
    dupl = duplicated(names(schema$rclass))
    schema$rclass = rclass = schema$rclass[!dupl]
    convert = TRUE
  }

  if (is.null(sql)) {
    if (length(table)==1 & tolower(substring(table[1],1,7))=="select ") {
      sql = table
    } else {
      if (is.null(where.sql))
        where.sql = sql.where.code(db, params, where.in=where.in)
      if (!is.null(orderby)) {
        orderby = paste0(" order by ",paste0(orderby, collapse=", "))
      }
      if (!is.null(fields)) {
        fields = paste0(fields, collapse=", ")
      } else {
        fields = "*"
      }
      if (length(table) > 1) {
        if (is.null(joinby))
          stop("If you specify more than one table, you either have to supply a custom SQL statement or specify explicit joinby arguments (and possibly fields).")
        join = paste0(" ",toupper(jointype), " JOIN ", table[-1], " USING(", paste0(joinby, collapse=", "),")", collapse="\n")
      } else {
        join = ""
      }

      sql = paste0('SELECT ', fields,' FROM ', table[1],join, where.sql, orderby)
    }
  }
  if (!run) return(sql)
  if (!where.in) {
    rs = dbSendQuery(db, sql, params=params)
  } else {
    rs = dbSendQuery(db, sql)
  }
  res = dbFetch(rs,n=n)
  dbClearResult(rs)
  if (NROW(res)==0 & empty.as.null) return(NULL)

  if (isTRUE(convert)) {
    res = convert.db.to.r(res,rclass=rclass, schema=schema, null.as.na=null.as.na, origin=origin)
  }

  res
}

#' Convert data from a database table to R format
#'
#' @param vals the values loaded from the database table
#' @param schema a table schema that can be used to convert values
#' @param rclass the r class of the table columns, is extracted from schema
#' @param null.as.na shall NULL values be converted to NA values?
#' @param origin the origin date for DATE and DATETIME conversion
convert.db.to.r = function(vals, rclass=schema$rclass, schema=NULL, as.data.frame=is.data.frame(vals), null.as.na=TRUE, origin = "1970-01-01") {
  restore.point("convert.db.to.r")

  names = names(rclass)
  if (is.data.frame(vals) & NROW(vals)==0) return(vals)

  res = suppressWarnings(lapply(names, function(name) {
    restore.point("hsfhkdhfkd")
    val = vals[[name]]
    if (is.null(val) & null.as.na) val = NA
    if (length(val)==0) {
      if (rclass[[name]]=="POSIXct" | rclass[[name]]=="Date") {
        class(val) = rclass[[name]]
        return(val)
      }
      return(as(val,rclass[[name]]))
    }

    new.val = try({
      # logicals may be in a string like "1" or "TRUE"
      if (rclass[[name]]=="logical") {
        res = as.logical(val)
        res[is.na(res)] = as.logical(as.numeric(val[is.na(res)]))
        return(res)
      }

      # If DATE and DATETIME are stored as numeric, we need an origin for conversion
      if ((is.numeric(val) | is.na(val)) & (rclass[[name]] =="Date" | rclass[[name]] =="POSIXct")) {
        if (is.na(val)) val = NA_real_
        if (rclass[[name]]=="Date") {
          as.Date(val,  origin = origin)
        } else {
          as.POSIXct(val, origin = origin)
        }
      } else {
        as(val,rclass[[name]])
      }
    })
    if (is(new.val,"try-error")) {
      stop(paste0("Error when trying to convert variable ", name, " to ", rclass[[name]],":\n", as.character(new.val)))
    }
    new.val

  }))
  names(res) = names
  if (as.data.frame) {
    vals = as.data.frame(vals)
    vals[,names(res)] = as.data.frame(res,stringsAsFactors=FALSE)
  } else {
    vals[names(res)] = res
  }
  vals
}

#' Convert data from a database table to R format
#'
#' @param vals the values loaded from the database table
#' @param schema a table schema that can be used to convert values
#' @param rclass the r class of the table columns, is extracted from schema
#' @param null.as.na shall NULL values be converted to NA values?
#' @param origin the origin date for DATE and DATETIME conversion
convert.r.to.db = function(vals, rclass=schema$rclass, schema=NULL, null.as.na=TRUE, origin = "1970-01-01", add.missing=TRUE) {
  restore.point("convert.r.to.db")

  if (add.missing) {
    names = names(rclass)
  } else {
    names = intersect(names(rclass),names(vals))
  }
  res = suppressWarnings(lapply(names, function(name) {
    val = vals[[name]]
    if (is.null(val) & null.as.na) val = NA

    new.val = try({
      # Blobs are converted via serialize and unserialize
      #if (rclass[[name]]=="blob") {
      #  return(I(serialize(val, connection=NULL)))
      #}

      # Use origin for DATE and DATETIME conversion
      if (rclass[[name]]=="Date") {
        as.Date(val,  origin = origin)
      } else if (rclass[[name]] =="POSIXct") {
        as.POSIXct(val, origin = origin)
      } else {
        as(val,rclass[[name]])
      }
    })

    if (is(new.val,"try-error")) {
      stop(paste0("Error when trying to convert variable ", name, " to ", rclass[[name]],":\n", as.character(new.val)))
    }
    new.val
  }))
  names(res) = names
  res
}

#' Convert an R object to a datetime object that can be used
#' in a WHERE clause.
to.db.datetime = function(val, origin = "1970-01-01") {
  as.numeric(as.POSIXct(val, origin = origin))
}

#' Convert an R object to a date object that can be used
#' in a WHERE clause.
to.db.date = function(val, origin = "1970-01-01") {
  as.numeric(as.Date(val, origin = origin))
}


#' Update a row in a database table
#'
#' @param db dbi database connection
#' @param table name of the table
#' @param vals named list of values to be inserted
#' @param where named list that specifies the keys where to update
#' @param schema a schema as R list, can be used to automatically convert types
#' @param sql optional a parameterized sql string
#' @param run if FALSE only return parametrized SQL string
#' @param rclass the r class of the table columns, is extracted from schema
#' @param convert if rclass is given shall results automatically be converted to these classes?
#' @param null.as.na shall NULL values be converted to NA values?
dbUpdate = function(db, table, vals,where=NULL, schema=schemas[[table]], schemas=get.db.schemas(db), sql=NULL,run=TRUE,  rclass=schema$rclass, convert=!is.null(rclass), null.as.na=TRUE,log.dir=NULL, do.log=!is.null(log.dir), user=NA, where.in=FALSE) {
  restore.point("dbUpdate")

  # Update vals based on table schema
  if (isTRUE(convert)) {
    vals = convert.r.to.db(vals,rclass = rclass,schema = schema,null.as.na = null.as.na, add.missing=FALSE)
    if (!is.null(where))
      where = convert.r.to.db(where,rclass = rclass,schema = schema,null.as.na = null.as.na, add.missing=FALSE)

  }
  cols = names(vals)

  if (is.null(sql)) {
    sql <- paste0("UPDATE ", table," SET ",
      paste0(cols, " = :",cols,collapse=", "))
    if (!is.null(where)) {
      where.sql = sql.where.code(db, where, where.in=where.in)
      sql <- paste0(sql," ",where.sql)
    }
  }
  if (!run) return(sql)
  ret = dbSendQuery(db, sql, params=c(vals,where))
  logDBcommand(user = user,type="update",sql=sql,log.dir=log.dir, do.log=do.log,table = table)

  invisible(list(values=vals))
}


#' Load and init database table schemas from yaml file
#'
#' @param file file name
#' @param yaml yaml as text
load.and.init.schemas = function(file=NULL, yaml= readLines(file,warn = FALSE)) {
  schemas = yaml.load(paste0(yaml, collapse="\n"))
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

#' Get a vector of R classes of the database columns described in a schema
#'
#' @param schema the schema
schema.r.classes = function(schema) {
  str = tolower(substring(schema$table,1,5))

  classes =c(
    chara = "character",
    text = "character",
    varch = "character",
    boole = "logical",
    integ = "integer",
    numer = "numeric",
    real = "numeric",
    doubl = "numeric",
    date = "Date",
    datet = "POSIXct"
    #blob = "blob"
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
  schema.template(row)
}

#' Create an example schema from a list of R objects
#'
#' The output is shown per cat and copied to the clipboard.
#' It can be used as a template for the .yaml schema file
#'
#' @param li The R list for which the schema shall be created
#' @param name optional a name of the table
#' @param toCliboard shall the created text be copied to the clipboard
schema.template = function(li, name="mytable", toClipboard=TRUE) {
  templ = c(
    "character" = "TEXT",
    "factor" = "TEXT",
    "integer" = "INTEGER",
    "numeric" = "NUMERIC",
    "logical" = "BOOLEAN",
    "POSIXct" = "DATETIME",
    "Date" = "DATE"
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
  table:
',paste0(cols,collapse='\n'),'
  index:
    - ',names(eli)[1],' # example index on first column
')

  stxt = sapply(names(li)[is.subli], function(name) {
    schema.template(li[[name]],name, toClipboard=FALSE)
  })
  txt = paste0(c(txt,stxt), collapse="\n")
  if (toClipboard) {
    if (.Platform$OS.type == "windows") {
      writeClipboard(txt)
    } else {
      if (require(clipr)) {
        clipr::write_clip(txt)
      } else {
        cat("\nTo write to clipboard on a non-windows OS, please first install the package 'clipr'.")
      }
    }
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
    "datetime" = as.POSIXct(NA),
    "Date" = as.Date(NA)
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

#' Create a parametrized or escaped SQL WHERE clause from
#' the provided parameters.
#'
#' @param db a database connection needed for correct escaping via glue_sql
#' @param params a list of parameters assume that db fields have the same name and have to be equal to provided values.
#' @param where.in Set true TRUE a member of params can be vector and return all rows that match an element. By default FALSE to generate more compact code.
#' @param paramertrized shall the generated code use SQL parameters.
#' @param add.where If TRUE start with WHERE
sql.where.code = function(db=NULL,params, where.in=FALSE, parametrized=!where.in, add.where=TRUE) {
  restore.point("sql.where.code")
  if (length(params)==0) return("")
  start = if (add.where) " WHERE " else (" ")
  if (parametrized & where.in)
    stop("Cannot combine parametrized queries with where.in")
  if (parametrized) {
    return(paste0(start, paste0(names(params)," = :",names(params), collapse= " AND ")))
  } else if (!parametrized & !where.in) {
    code = paste0(start, paste0(names(params)," = ({",names(params),"})", collapse= " AND "))
    sql = glue_sql(code,.con=db,.envir=params)
    return(sql)
  } else if (where.in) {
    code = paste0(start, paste0(names(params)," IN ({",names(params),"*})", collapse= " AND "))
    sql = glue_sql(code,.con=db,.envir=params)
    return(sql)
  }
}



#' Get a data frame with column information for a database table
#'
#' @param db dbi database connection
#' @param table name of the table
dbTableCols = function(db, table) {
  sql = paste0("PRAGMA table_info(",table,");")
  #sql = paste0("SELECT sql FROM sqlite_master WHERE tbl_name = '", table,"' AND type = 'table'")
  rs = dbSendQuery(db, sql)
  dbFetch(rs)
}

