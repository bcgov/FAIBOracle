#' Get the project date for the rank 1 layer.
#'
#'
#' @description This function is to query the project date for the rank 1 layer.
#'              Use this as version indication.
#'
#' @param bcgwUserName character, Specifies a valid user name for bcgw database.
#' @param bcgwPassword character, Specifies the password to the user name.
#'
#' @return project date for the rank 1 layer
#'
#' @importFrom ROracle dbConnect dbGetQuery dbDisconnect
#' @importFrom DBI dbDriver
#' @export
#' @rdname projDate_rank1Layer
#' @author Yong Luo
projDate_rank1Layer <- function(bcgwUserName,
                                 bcgwPassword){
  drv <- dbDriver("Oracle")
  connect_to_bcgw <- "(DESCRIPTION=(ADDRESS_LIST = (ADDRESS = (PROTOCOL = TCP)
      (HOST = bcgw.bcgov)(PORT = 1521)))
      (CONNECT_DATA = (SERVICE_NAME = idwprod1.bcgov)))"
  con <- dbConnect(drv,
                   username = bcgwUserName,
                   password = bcgwPassword,
                   dbname = connect_to_bcgw)
  projectdate <- dbGetQuery(con,
                        paste0("SELECT
                          a.PROJECTED_DATE
                          FROM
                          whse_forest_vegetation.veg_comp_lyr_r1_poly a
                           FETCH FIRST 1 ROWS ONLY"))
  dbDisconnect(con)
  return(projectdate$PROJECTED_DATE)
}

