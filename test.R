#' Using Package
#'
#' This function use to install the package.
#' @param p A package that will installed to be.
#' @keywords install
#' @export
#'


usePackage <- function(p) {
  if (!is.element(p, installed.packages()[, 1]))
    install.packages(p, dep = TRUE)
  require(p, character.only = TRUE)
}


usePackage("FRACTION")
'%ni%' <- Negate('%in%')


#' Create table
#'
#' This function use to create table sql.
#' @param df Name of a data frame.
#' @param table_name The hive table name.
#' @param partition_columns=c The column that you want to partition.
#' @return
#' @export
#'

# Returns a create table statement given ----------------------------------
create_tbl_sql <- function(df, table_name, partition_columns=c()) {

  # Build columns statement
  columns_stmt <- ""
  for (column in colnames(df)) {
    if (column %ni% partition_columns)
      datatype <- map_datatype(df[[column]])
    columns_stmt <- paste(columns_stmt, column, datatype, ", ")
  }
  columns_stmt <- substr(columns_stmt, 1, nchar(columns_stmt) - 2)
  columns_stmt <- paste0("(", columns_stmt, ")")


  if (length(partition_columns) != 0) {

    # Check if partition columns is in DataFrame
    has_all_columns <- TRUE
    for (column in partition_columns) {

      if (!(column %in% colnames(df))) {
        print(paste("Error", column, "is not in df"))
        has_all_columns <- FALSE
        return(FALSE)
      }
    }

    # Build partition stmt in create table query
    partition_columns_stmt <- ""
    for (column in partition_columns) {
      datatype <- map_datatype(df[[column]])
      partition_columns_stmt <- paste(partition_columns_stmt, column, datatype, ", ")
    }
    partition_columns_stmt <- substr(partition_columns_stmt, 1, nchar(partition_columns_stmt) - 2)
    partition_columns_stmt <- paste0("(", partition_columns_stmt, ")")


    sql = paste("CREATE TABLE", table_name, columns_stmt, "PARTITIONED BY ",
                partition_columns_stmt, "STORED AS ORC")

  } else {
    sql = paste("CREATE TABLE", table_name, columns_stmt, "STORED AS ORC")
  }
  return(sql)
}


#' Map R datatype to hive
#'
#' This function use to change the Map R data type to Hive data type.
#' @param df_column Each column of the data frame.
#' @return
#' @export
#'

# Map R datatype to hive -------------------------------------------------
map_datatype <- function(df_column) {
  # Map the R Column Data Types to Hive Data types
  type <- class(df_column)
  if (type == "character") {
    return("string")
  } else if (type == "numeric") {
    if (is.wholenumber(df_column)) {
      return("int")
    } else {
      return("double")
    }
  } else if (type == "integer") {
    return("int")
  } else if (type == "logical") {
    return("boolean")
  } else if (type == "Date") {
    return("date")
  } else if (type == "factor") {
    return("string")
  } else if (type[1] == "POSIXct" || type[2] == "POSIXt") {
    return("timestamp")
  } else {
    print(paste("Type", type, "is not on config."))
  }
}

#' R to Hive
#'
#' This loads an R data frame to hive table.
#' @param df The R data frame.
#' @param table_name The hive table name.
#' @param server A connection to RODBC.
#' @return
#' @export
#' @examples
#'   table_name = dsa_prod.test
#'   server <- odbcConnect("MapRHive;uid=asconde;pwd=asconde")
#'

# r_to_hive ---------------------------------------------------------
r_to_hive <- function(df, table_name, server, mode="replace", partition_columns=c()) {
  # This loads an R dataframe to hive table
  # Arguments:
  #   df -- R dataframe
  #   table_name -- hive table name. ex: dsa_prod.test
  #   server -- connection to RODBC


  # 0: Load needed libraries
  usePackage('httr')
  usePackage('rccmisc')
  usePackage('data.table')
  usePackage('RODBC')

  # 0: Check if the database provided is allowed to write on
  allowed_dbs <- c("datamart_dev", "datamart_prod", "dsa_bicm", "dsa_cqm", "dsa_dao", "dsa_monopoly", "dsa_prod", "dsa_test")
  db_to_write_on <- strsplit(table_name, ".", fixed=TRUE)[[1]][1]
  if (db_to_write_on %ni% allowed_dbs) {
    print(paste("Data Science team is not allowed to write on", db_to_write_on))
    return(FALSE)
  }

  # 0: Set variables
  flask_bash_url <- "http://10.51.2.73:8123"
  hdfs_staging_folder <- "/datalake/data_science/staging/"

  # 0: Check if what OS the environment is running and set the directories
  if (.Platform$OS.type == "unix") {
    tmp_folder <- "/samba/shared/Libraries/R/r_to_hive/tmp/"
  } else { # If running on windows
    tmp_folder <- "//10.51.2.73/Shared/Libraries/R/r_to_hive/tmp/"
  }


  # 1: Export RDS Dataframe to CSV
  tmp_path <- paste0(tmp_folder, table_name, ".csv")
  fwrite(df, file=tmp_path, sep="\u0001", col.names=FALSE)

  # 2: Create a folder in HDFS to place csv for staging
  hdfs_staging_path <- paste0(hdfs_staging_folder, table_name)
  cmd_mkdir <- paste0("hdfs dfs -mkdir ", hdfs_staging_path)
  print(cmd_mkdir)
  GET(flask_bash_url, query = list(command = cmd_mkdir))

  # 3: Put the csv to hdfs by calling a "flask bash" instance pointing to new data lake
  server_tmp_path <- paste0("/opt/samba/shared/Libraries/R/r_to_hive/tmp/", table_name, ".csv")
  cmd_put_csv <- paste0("hdfs dfs -put -f ", server_tmp_path, " ", hdfs_staging_path)
  GET(flask_bash_url, query = list(command = cmd_put_csv))

  # 4: Create staging table
  columns_stmt <- ""
  for (col in  colnames(df)) {
    hive_type <- map_datatype(df[[col]])
    columns_stmt <- paste(columns_stmt, col, hive_type, ",")
  }
  columns_stmt <- substr(columns_stmt, 1, nchar(columns_stmt) - 1)

  # 5: Create landing table
  sql_create_landing_tbl <- paste0("CREATE TABLE IF NOT EXISTS ", table_name, "_landing",
                                   " (", columns_stmt, ") ",
                                   "ROW FORMAT DELIMITED ",
                                   "FIELDS TERMINATED BY '\u0001' ",
                                   "STORED AS TEXTFILE ",
                                   "LOCATION '", hdfs_staging_path, "';")
  print(sql_create_landing_tbl)
  sqlQuery(server, sql_create_landing_tbl)


  # 6: Create orc table
  sql_create_orc_tbl <- create_tbl_sql(df, table_name, partition_columns = partition_columns)
  sqlQuery(server, sql_create_orc_tbl)


  # 7: Insert data from landing table to orc table
  # Check the mode if overwrite or append the perform the query
  counts_before <- 0
  if (mode == "append") {
    # Get counts before append -- for validation
    sql_counts <- paste("SELECT COUNT(*) FROM", table_name)
    counts_before <- sqlQuery(server, sql_counts)[[1]]

    sql_insert_from_landing_to_orc <- paste0("INSERT INTO TABLE ", table_name, " SELECT * FROM ", table_name, "_landing;")
  } else {
    sql_insert_from_landing_to_orc <- paste0("INSERT OVERWRITE TABLE ", table_name, " SELECT * FROM ", table_name, "_landing;")
  }
  sqlQuery(server, sql_insert_from_landing_to_orc)


  # 8: Drop landing table
  sql_drop_landing_tbl <- paste0("DROP TABLE IF EXISTS ", table_name, "_landing;")
  sqlQuery(server, sql_drop_landing_tbl)


  # 9: Check counts
  sql_counts <- paste("SELECT COUNT(*) FROM", table_name)
  counts <- sqlQuery(server, sql_counts)[[1]]

  # 10: Remove the tmp file
  file.remove(tmp_path)


  # 11: Return true if the counts of df and the created hive table is the same
  if (mode == "append") {
    is_same_counts <- counts == (nrow(df) + counts_before)
  } else {
    is_same_counts <- counts == nrow(df)
  }

  if (is_same_counts == TRUE) {
    print(paste("Table is loaded as expected", nrow(df) + counts_before, "/", counts))
    print(paste("Mode:", mode))
  } else {
    print(paste("Counts are not same.", nrow(df) + counts_before, "/", counts))
  }

  return(is_same_counts)
}
