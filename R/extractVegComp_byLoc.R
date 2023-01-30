#' Load the attributes from vegcomp rank 1 layer from bcgw warehouse by location.
#'
#'
#' @description This function is to extract the attributes from vegcomp rank 1
#'              layer from bcgw warehouse for given locations.
#'
#' @param bcgwUserName character, Specifies a valid user name for bcgw database.
#' @param bcgwPassword character, Specifies the password to the user name.
#' @param locationID character, Specifies the location id.
#' @param x_BCAlbers numeric, Specifies x coordinate for the location,
#'                            It must be in BC albers projection.
#' @param y_BCAlbers numeric, Specifies y coordinate for the location,
#'                            It must be in BC albers projection.
#' @param extractAttributes character, The attributes that are intended to extract from vegcomp layer.
#'                                     The entire list of attributes can be found at \href{https://www2.gov.bc.ca/assets/gov/farming-natural-resources-and-industry/forestry/stewardship/forest-analysis-inventory/data-management/standards/vegcomp_toc_data_dictionaryv5_2019.pdf}{here}
#'
#' @return a data table with the attributes
#'
#' @importFrom data.table ':=' data.table set
#' @importFrom dplyr '%>%'
#' @importFrom ROracle dbConnect dbGetQuery dbDisconnect
#' @importFrom DBI dbDriver
#' @export
#' @rdname extractVegComp_byLoc
#' @author Yong Luo
extractVegComp_byLoc <- function(bcgwUserName,
                                 bcgwPassword,
                                 locationID,
                                 x_BCAlbers,
                                 y_BCAlbers,
                                 extractAttributes){
  thetable <- data.table(locationID,
                         x_BCAlbers,
                         y_BCAlbers)

  drv <- dbDriver("Oracle")
  connect_to_bcgw <- "(DESCRIPTION=(ADDRESS_LIST = (ADDRESS = (PROTOCOL = TCP)
      (HOST = bcgw.bcgov)(PORT = 1521)))
      (CONNECT_DATA = (SERVICE_NAME = idwprod1.bcgov)))"
  con <- dbConnect(drv,
                   username = bcgwUserName,
                   password = bcgwPassword,
                   dbname = connect_to_bcgw)
  allfid <- NULL
  for(i in 1:nrow(thetable)){
    indifid <- dbGetQuery(con,
                          paste0("SELECT
                          a.feature_id
                          FROM
                          whse_forest_vegetation.veg_comp_lyr_r1_poly a

                          where
                          SDO_CONTAINS(a.geometry,
                                   SDO_GEOMETRY(2001, 3005, SDO_POINT_TYPE(",
                                 thetable$x_BCAlbers[i],
                                 ", ",
                                 thetable$y_BCAlbers[i],
                                 ", NULL), NULL, NULL)) = 'TRUE'")) %>%
      data.table
    if(nrow(indifid) > 0){
      indifid <- cbind(data.table(locationID = thetable$locationID[i]),
                       indifid)
    } else {
      indifid <- cbind(data.table(locationID = thetable$locationID[i]),
                       data.table(FEATURE_ID = NA))
    }
    allfid <- rbind(allfid,
                    indifid)
  }
  no_feature_id <- allfid[is.na(FEATURE_ID),]
  allfidlist <- unique(allfid[!is.na(FEATURE_ID)]$FEATURE_ID)
  dbDisconnect(con)
  if(length(allfidlist) > 0){
    ## as the maximum number of feature_id is 1000 for each query
    ## need to break the whole list if pass this number
    allcuts <- sort(unique(c(seq(0, length(allfidlist), by = 999), length(allfidlist))))
    allattri <- NULL
    for (j in 1:(length(allcuts)-1)) {
      indiattri <- extractVegComp_byFID(bcgwUserName,
                                        bcgwPassword,
                                        FID = allfidlist[(allcuts[j]+1):allcuts[(j+1)]],
                                        extractAttributes)
      allattri <- rbind(allattri,
                        indiattri)
      rm(indiattri)
      gc()
    }
    allattri <- merge(allfid,
                      allattri,
                      by = "FEATURE_ID",
                      all.x = TRUE)
  } else {
    allattri <- no_feature_id
    data.table::set(allattri, , extractAttributes, NA)
  }
  alloutput <- merge(thetable,
                     allattri,
                     by = "locationID",
                     all.x = TRUE)
  return(alloutput)
}








