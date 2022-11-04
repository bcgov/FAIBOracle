#' Load the attributes from vegcomp rank 1 layer from bcgw warehouse.
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
#' @importFrom data.table ':=' data.table year
#' @importFrom dplyr '%>%'
#' @importFrom ROracle dbConnect dbGetQuery dbDisconnect
#' @importFrom DBI dbDriver
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
    indifid <- cbind(data.frame(locationID = thetable$locationID[i]),
                     indifid)
    allfid <- rbind(allfid,
                    indifid)
  }
  allfidlist <- unique(allfid$FEATURE_ID)

  dbDisconnect(con)
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
  alloutput <- merge(thetable,
                     allattri,
                     by = "locationID",
                     all.x = TRUE)
  return(alloutput)
}


#' Load the attributes from vegcomp rank 1 layer from bcgw warehouse.
#'
#'
#' @description This function is to extract the attributes from vegcomp rank 1
#'              layer from bcgw warehouse for given feature id
#'
#' @param bcgwUserName character, Specifies a valid user name for bcgw database.
#' @param bcgwPassword character, Specifies the password to the user name.
#' @param FID character, Specifies the feature_id in vegcomp rank1 layer.
#' @param extractAttributes character, The attributes that are intended to extract from vegcomp layer.
#'                                     The entire list of attributes can be found at \href{https://www2.gov.bc.ca/assets/gov/farming-natural-resources-and-industry/forestry/stewardship/forest-analysis-inventory/data-management/standards/vegcomp_toc_data_dictionaryv5_2019.pdf}{here}
#'
#' @return a data table with the attributes
#' @note the maximum number of feature id is 1000
#' @importFrom data.table ':=' data.table year
#' @importFrom dplyr '%>%'
#' @importFrom ROracle dbConnect dbGetQuery dbDisconnect
#' @importFrom DBI dbDriver
#' @rdname extractVegComp_byFID
#' @author Yong Luo
extractVegComp_byFID <- function(bcgwUserName,
                                 bcgwPassword,
                                 FID,
                                 extractAttributes){
  if(length(FID) == 1){
    FID <- paste0("('", FID,"')")
  } else {
    FID <- paste0("('", paste0(FID, collapse = "', '"),"')")
  }

  if(!("Feature_ID" %in% extractAttributes)){
    extractAttributes <- c("Feature_ID", extractAttributes)
  }
  extractAttributes <- paste0("a.", extractAttributes)
  extractAttributes <- paste(extractAttributes, collapse = ", ")
  drv <- dbDriver("Oracle")
  connect_to_bcgw <- "(DESCRIPTION=(ADDRESS_LIST = (ADDRESS = (PROTOCOL = TCP)
      (HOST = bcgw.bcgov)(PORT = 1521)))
      (CONNECT_DATA = (SERVICE_NAME = idwprod1.bcgov)))"
  con <- dbConnect(drv,
                   username = bcgwUserName,
                   password = bcgwPassword,
                   dbname = connect_to_bcgw)
  theoutput <- dbGetQuery(con,
                          paste0("SELECT
                                 ",
                                 extractAttributes,

                                 "
                                 FROM
                                 whse_forest_vegetation.veg_comp_lyr_r1_poly a

                                 WHERE
                                 a.Feature_ID in ", FID)) %>%
    data.table
  dbDisconnect(con)
  return(theoutput)
}






