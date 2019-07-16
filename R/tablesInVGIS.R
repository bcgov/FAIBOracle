#' Obtain all the table names (and column names) in VGIS
#'
#' @description This function is to load all the table names from VGIS database.
#'
#' @param userName character, Specifies a valid user name in VGIS Oracle database.
#' @param passWord character, Specifies the password to the user name.
#' @param columnNames logic, Obtain column names in each table. Default is TRUE.
#'
#' @return a data table
#'
#' @importFrom data.table ':=' data.table
#' @importFrom dplyr '%>%'
#' @importFrom ROracle dbConnect dbGetQuery dbDisconnect dbListTables
#' @importFrom DBI dbDriver
#' @export
#'
#' @seealso \code{\link{tablesInISMC}} and \code{\link{tablesInGYS}}
#' @rdname tablesInVGIS
#' @author Yong Luo
tablesInVGIS <- function(userName, passWord, columnNames = TRUE){
  drv <- dbDriver("Oracle")
  connect.string <-"(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)
  (HOST=nrk1-scan.bcgov)(PORT=1521))
  (CONNECT_DATA=(SERVER=DEDICATED)(SERVICE_NAME=ENVPROD1.NRS.BCGOV)))"
  con <- dbConnect(drv, username = userName, password = passWord,
                   dbname = connect.string)
  alltables <- ROracle::dbListTables(con, all = TRUE, full = TRUE)
  alltables <- data.table::data.table(matrix(alltables, ncol = 2))
  names(alltables) <- c("DataBase", "TableName")
  alltables <- alltables[DataBase == "VGIS",]

  if(columnNames){
    alltables_new <- data.table::copy(alltables)
    alltables_new[, oracleName := paste0(DataBase, ".", TableName)]
    alltables_new <- alltables_new[!(oracleName %in% c("VGIS.PLAN_TABLE",
                                                       "VGIS.BUSINESS_RULE_PLSQL_SOURCE",
                                                       "VGIS.BUSINESS_RULE_PLSQL_SOURCE_JN",
                                                       "VGIS.FIELD_DATA_BIT_MAP_IMAGES",
                                                       "VGIS.GPS_FILES"))]
    alltables <- data.table(DataBase = character(),
                            TableName = character(),
                            columnName = character())
    # browser()
    for(i in 1:nrow(alltables_new)){
      indirow <- alltables_new$oracleName[i]
      cat(indirow, "\n")
      thetable <- dbGetQuery(con,
                             paste0("select * from ", indirow,
                                    " fetch first 0 rows only"))
      alltables <- rbind(alltables,
                         data.table(DataBase = alltables_new$DataBase[i],
                                    TableName = alltables_new$TableName[i],
                                    columnName = names(thetable)))
      rm(thetable)
    }
    rm(alltables_new)
  }
  dbDisconnect(con)
  return(alltables)

}
