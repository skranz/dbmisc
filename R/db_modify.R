
#' Create or update a SQLite database from a schema file
#'
#' @param schema.file the dbmisc schema file in yaml format
#' @param schema.dir the directory of the schema file (if schema.file does not contain a path)
#' @param db.name the name of the database file
#' @param db.dir the directory of the database file, by default the schema directory
#' @param update if TRUE (default) copy and update the existing data in the tables. If FALSE just generate empty tables
#' @export
dbCreateSQLiteFromSchema = function(schema.file, schema.dir=dirname(schema.file), db.name=NULL, db.dir=schema.dir, update=TRUE) {
  restore.point("dbCreateSQLiteFromSchema")


  schema.file = basename(schema.file)
  schemas = load.and.init.schemas(file=file.path(schema.dir,schema.file))

  if (is.null(db.name)) {
    db.name = paste0(tools::file_path_sans_ext(schema.file),".sqlite")
  }
  library(RSQLite)
  db = dbConnect(RSQLite::SQLite(),dbname=file.path(db.dir,db.name))
  dbCreateSchemaTables(db,schemas = schemas, update=update)
  dbDisconnect(db)
  cat("\nGenerated",file.path(db.dir,db.name))
  file.path(db.dir,db.name)
}


#' Create or update database tables and possible indices from a simple yaml schema
#'
#' @param db dbi database connection
#' @param schemas schemas as R list
#' @param schema.yaml alternatively a schema as yaml text
#' @param schema.file alternatively a file name of a schema yaml file
#' @param overwrite shall existing tables be overwritten?
#' @param update if TRUE (default) copy old data from existing tables.
#' @param silent if TRUE don't show messages
dbCreateSchemaTables = function(db,schemas=get.db.schemas(db), schema.yaml=NULL, schema.file=NULL, overwrite=update,silent=FALSE, update=TRUE) {
  restore.point("dbCreateSchemaTables")

  if (is.null(schemas)) {
    if (is.null(schema.yaml))
      schema.yaml = readLines(schema.file,warn = FALSE)
    schema.yaml = paste0(schema.yaml, collapse = "\n")
    schemas = yaml.load(schema.yaml)
  }

  tables = names(schemas)
  lapply(tables, function(table) {
    dbCreateSchemaTable(db, table, schemas=schemas, overwrite=overwrite, silent=silent)
  })
  invisible(schemas)
}

#' Create database table and possible indices from a simple yaml schema
#'
#' @param db dbi database connection
#' @param schemas schemas as R list
#' @param schema.yaml alternatively a schema as yaml text
#' @param schema.file alternatively a file name of a schema yaml file
#' @param overwrite shall existing tables be overwritten?
#' @param update shall old data be copied from existing tables?
#' @param silent if TRUE don't show messages
dbCreateSchemaTable = function(db,table, schema=schemas[[table]], schemas=get.db.schemas(db), schema.yaml=NULL, schema.file=NULL, overwrite=update,silent=FALSE, update=TRUE) {
  restore.point("dbCreateSchemaTable")

  has.dat = FALSE
  if (update) {
    if (dbExistsTable(db, table)) {
      has.dat = TRUE
      dat = dbGet(db, table)
    }
  }

  if (overwrite | update)
    try(dbRemoveTable(db, table), silent=silent)

  if (dbExistsTable(db,table)) return(FALSE)
  # create table
  sql = paste0("CREATE TABLE ", table,"(",
    paste0(names(schema$table), " ", schema$table, collapse=",\n"),
    ")"
  )
  dbSendQuery(db,sql)

  # create indexes specified as SQL
  for (index in schema[["indexes"]]) {
    err = try(dbSendQuery(db,index), silent=TRUE)
    if (is(err,"try-error")) {
      msg = as.character(err)
      msg = str.right.of(msg,"Error :")
      msg = paste0("When running \n", index,"\n:\n",msg)
      stop(msg)
    }
  }

  count = 0
  # create indexes specified as columns
  for (index in schema[["index"]]) {
    count = count + 1
    sql = paste0("CREATE INDEX ind_",table,"_",count , " ON ", table, " (", paste0(index, collapse=", "),")")
    err = try(dbSendQuery(db,sql), silent=TRUE)
    if (is(err,"try-error")) {
      msg = as.character(err)
      msg = str.right.of(msg,"Error :")
      msg = paste0("When running \n", sql,"\n:\n",msg)
      stop(msg)
    }
  }
  if (has.dat) {
    dbInsert(db, table, dat,schema = schema)
  }

  return(TRUE)
}

