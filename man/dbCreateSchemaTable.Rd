% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/db_modify.R
\name{dbCreateSchemaTable}
\alias{dbCreateSchemaTable}
\title{Create database table and possible indices from a simple yaml schema}
\usage{
dbCreateSchemaTable(
  db,
  table,
  schema = schemas[[table]],
  schemas = get.db.schemas(db),
  schema.yaml = NULL,
  schema.file = NULL,
  overwrite = update,
  silent = FALSE,
  update = TRUE,
  verbose = 1
)
}
\arguments{
\item{db}{dbi database connection}

\item{schemas}{schemas as R list}

\item{schema.yaml}{alternatively a schema as yaml text}

\item{schema.file}{alternatively a file name of a schema yaml file}

\item{overwrite}{shall existing tables be overwritten?}

\item{silent}{if TRUE don't show messages}

\item{update}{shall old data be copied from existing tables?}
}
\description{
Create database table and possible indices from a simple yaml schema
}
