#' Load all DBH-Height data from ISMC Oracle database
#'
#'
#' @description This function is to load all the DBHG-height data regardless of
#'              sample site visit type. Note that this function is specifically used for
#'              loading DBH and Height for deriving DBH-height function that used
#'              in the PSP and VRI compiler. Hence, the broken top trees are excluded
#'              in the dataset. The data is limited for the five major repeatedly
#'              measured projects, i.e., 'PSP', 'M', 'Y', 'L', 'F'. The I and S samples
#'              in the PSP projects also excluded.
#'
#' @param userName character, Specifies a valid user name in ISMC Oracle database.
#' @param passWord character, Specifies the password to the user name.
#' @param env character, Specifies which environment the data reside. Currently,
#'                               the function supports \code{INT} (intergration)
#'                               and \code {TEST} (test) environment.
#'
#'
#' @return DBH and height table.
#'
#' @importFrom data.table ':=' data.table
#' @importFrom dplyr '%>%'
#' @importFrom ROracle dbConnect dbGetQuery dbDisconnect
#' @importFrom DBI dbDriver
#' @export
#'
#' @rdname loadISMC_sizedata
#' @author Yong Luo
loadISMC_sizedata <- function(userName, passWord, env){

  drv <- dbDriver("Oracle")
  connect_to_ismc <- getServer(databaseName = "ISMC", envir = env)
  con <- dbConnect(drv, username = userName,
                   password = passWord,
                   dbname = connect_to_ismc)
  thedata <-
    dbGetQuery(con,
               paste0("select
                      ss.site_identifier,
                      ssn.SAMPLE_SITE_NAME,
                      ssv.visit_number,
                      ssv.sample_site_purpose_type_code,
                      ssv.SAMPLE_SITE_VISIT_START_DATE,
                      plc.utm_zone,
                      plc.utm_northing,
                      plc.utm_easting,
                      plc.elevation,
                      plc.point_location_type_code,
                      plc.coordinate_source_code,
                      pt.plot_category_code,
                      pt.plot_number,
                      tr.tree_number,
                      tr.felled_ind,
                      td.tree_species_code,
                      tm.tree_extant_code,
                      tm.tree_class_code,
                      tm.diameter_source_code,
                      tm.height_source_code,
                      tm.diameter_measmt_height,
                      tm.measurement_anomaly_code,
                      tm.diameter,
                      tm.broken_top_ind,
                      tm.length


                      from
                      app_ismc.tree_measurement tm

                      left join app_ismc.tree tr
                      on tr.tree_guic = tm.tree_guic

                      left join app_ismc.plot pt
                      on pt.plot_guic = tr.plot_guic

                      left join app_ismc.sample_measurement sm
                      on sm.sample_measurement_guic = tm.sample_measurement_guic

                      left join app_ismc.sample_site_visit ssv
                      on ssv.sample_site_visit_guic = sm.sample_site_visit_guic

                      left join app_ismc.tree_detail td
                      on td.tree_guic = tm.tree_guic and
                      td.sample_site_visit_guic = sm.sample_site_visit_guic

                      left join app_ismc.sample_site ss
                      on ss.sample_site_guic = ssv.sample_site_guic

                      left join app_ismc.point_location plc
                      on plc.point_location_guic = ss.point_location_guic

                      left join app_ismc.conv_sample_site_name ssn
                      on ssn.sample_site_guic = ss.sample_site_guic

                      left join app_ismc.psp_sample_site pspss
                      on pspss.sample_site_guic = ss.sample_site_guic

                      left join app_ismc.ground_sample_project gsp
                      on gsp.ground_sample_project_guic = ssv.ground_sample_project_guic

                      where
                      ssv.sample_site_purpose_type_code in ('PSP', 'M', 'Y', 'L', 'F')
                      and tm.broken_top_ind in ('N')
                      and ssv.SAMPLE_SITE_VISIT_STATUS_CODE in ('ACC', 'APP', 'INACTIVE')
                      and (pspss.PSP_SAMPLE_SITE_TYPE_CODE not in ('S', 'I')
                      or pspss.PSP_SAMPLE_SITE_TYPE_CODE is null)")) %>%
    data.table
  return(thedata)
}
