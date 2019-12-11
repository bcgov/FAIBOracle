#' Obtain all the table names (and column names) in ISMC
#'
#' @description This function is to load all the table names from ISMC database.
#'
#' @param userName character, Specifies a valid user name in ISMC Oracle database.
#' @param passWord character, Specifies the password to the user name.
#' @param env character, Specifies which environment the data reside. Currently,
#'                               the function supports \code{INT} (intergration)
#'                               and \code {TEST} (test) environment.
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
#' @seealso \code{\link{tablesInGYS}} and \code{\link{tablesInVGIS}}
#' @rdname tablesInISMC
#' @author Yong Luo
tablesInISMC <- function(userName, passWord, env, columnNames = TRUE){
  drv <- dbDriver("Oracle")
  connect_to_ismc <- getServer(databaseName = "ISMC",
                                 envir = env)
  con <- dbConnect(drv, username = userName,
                   password = passWord,
                   dbname = connect_to_ismc)
  alltables <- ROracle::dbListTables(con, all = TRUE, full = TRUE)
  alltables <- data.table::data.table(matrix(alltables, ncol = 2))
  names(alltables) <- c("DataBase", "TableName")
  alltables <- alltables[DataBase == "APP_ISMC",]
  if(columnNames){
    alltables_new <- data.table::copy(alltables)
    alltables_new[, oracleName := paste0(DataBase, ".", TableName)]
    alltables_new <- alltables_new[!(oracleName %in% c("APP_ISMC.POINT_LOCATION",
                                                     "APP_ISMC.BIOGEOCLIMATIC_POLY_SHADOW"))]
    alltables <- data.table(DataBase = character(),
                            TableName = character(),
                            columnName = character())
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
