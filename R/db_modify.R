#' Create or update a SQLite database from a schema file
#'
#' @param schema.file the dbmisc schema file in yaml format
#' @param schema.dir the directory of the schema file (if schema.file does not contain a path)
#' @param db.name the name of the database file
#' @param db.dir the directory of the database file, by default the schema directory
#' @param update if TRUE copy and update the existing data in the tables. If FALSE just generate empty tables.
#' @param verbose if 0 don't show what is done. If 1 or larger show most of the run SQL commands.
#' @export
dbCreateSQLiteFromSchema = function(schema.file, schema.dir=dirname(schema.file), db.name=NULL, db.dir=schema.dir, update=TRUE, verbose=1) {
  restore.point("dbCreateSQLiteFromSchema")


  schema.file = basename(schema.file)
  schemas = load.and.init.schemas(file=file.path(schema.dir,schema.file))

  if (is.null(db.name)) {
    db.name = paste0(tools::file_path_sans_ext(schema.file),".sqlite")
  }
  db.file = file.path(db.dir,db.name)
  did.exist = file.exists(db.file)
  if (!did.exist) {
    update = FALSE
  }

  library(RSQLite)
  db = dbConnect(RSQLite::SQLite(),dbname=db.file)
  dbCreateSchemaTables(db,schemas = schemas, update=update,overwrite=TRUE, verbose=verbose)
  dbDisconnect(db)
  if (did.exist & update) {
    cat("\nUpdated existing data base ",db.file)
  } else if (did.exist) {
    cat("\nOverwrote the existing data base ",db.file)
  } else {
    cat("\nGenerated the new data base ",db.file)
  }
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
dbCreateSchemaTables = function(db,schemas=get.db.schemas(db), schema.yaml=NULL, schema.file=NULL, overwrite=update,silent=FALSE, update=TRUE, verbose=1) {
  restore.point("dbCreateSchemaTables")

  if (is.null(schemas)) {
    if (is.null(schema.yaml))
      schema.yaml = readLines(schema.file,warn = FALSE)
    schema.yaml = paste0(schema.yaml, collapse = "\n")
    schemas = yaml.load(schema.yaml)
  }

  tables = names(schemas)
  lapply(tables, function(table) {
    dbCreateSchemaTable(db, table, schemas=schemas, overwrite=overwrite, silent=silent,update=update, verbose=verbose)
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
dbCreateSchemaTable = function(db,table, schema=schemas[[table]], schemas=get.db.schemas(db), schema.yaml=NULL, schema.file=NULL, overwrite=update,silent=FALSE, update=TRUE, verbose=1) {
  restore.point("dbCreateSchemaTable")

  #known.fields = c("table","index","unique_index","indexes","sql")
  #extra.fields = setdiff(names(schema), known.fields)
  #if (length(extra.fields) > 0) {
  #  warning("The schema for table ", table, " specifies the unknown field(s) ", paste0(extra.fields, collapse=", "),". Only the fields ", paste0(known.fields, collapse=", "), " are known and will be used.")
  #}

  has.dat = FALSE
  if (update) {
    if (dbExistsTable(db, table)) {
      has.dat = TRUE
      dat = dbGet(db, table, schema = schema, schemas=NULL)
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
  if (verbose > 0) cat(paste0("\n\n\n", sql))

  dbSendQuery(db,sql)



  # create indexes specified as SQL
  for (index in c(schema[["indexes"]], schema[["sql"]])) {
    if (verbose > 0) cat("\n", sql)
    err = try(dbSendQuery(db,index), silent=TRUE)
    if (is(err,"try-error")) {
      msg = as.character(err)
      msg = str.right.of(msg,"Error :")
      msg = paste0("When running \n", index,"\n:\n",msg)
      stop(msg)
    }
  }

  count = 0
  # create unique indexes specified as columns
  for (index in schema[["unique_index"]]) {
    count = count + 1
    sql = paste0("CREATE UNIQUE INDEX ind_",table,"_",count , " ON ", table, " (", paste0(index, collapse=", "),")")
    if (verbose>0) cat(paste0("\n\n", sql))
    err = try(dbSendQuery(db,sql), silent=TRUE)
    if (is(err,"try-error")) {
      msg = as.character(err)
      msg = str.right.of(msg,"Error :")
      msg = paste0("When running \n", sql,"\n:\n",msg)
      stop(msg)
    }
  }


  # create indeces specified as columns
  for (index in schema[["index"]]) {
    count = count + 1
    sql = paste0("CREATE INDEX ind_",table,"_",count , " ON ", table, " (", paste0(index, collapse=", "),")")
    if (verbose>0) cat(paste0("\n\n", sql))
    err = try(dbSendQuery(db,sql), silent=TRUE)
    if (is(err,"try-error")) {
      msg = as.character(err)
      msg = str.right.of(msg,"Error :")
      msg = paste0("When running \n", sql,"\n:\n",msg)
      stop(msg)
    }
  }


  if (has.dat) {
    if (verbose > 0) cat("\n\nInsert ", NROW(dat), " rows of old data into ",table)
    dbInsert(db, table, dat,schema = schema, schemas=NULL)
  }

  return(TRUE)
}

