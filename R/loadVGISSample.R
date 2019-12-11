#' Load the sample header data from Oracle database
#'
#'
#' @description This function is to load the sample header data from VGIS Oracle database. And
#'              associated them with cluster.
#'
#' @param userName character, Specifies a valid user name in VGIS Oracle database.
#' @param passWord character, Specifies the password to the user name.
#' @param saveThem logical, Specifies whether the loaded data should be saved or returned.
#'                 The default value is FALSE, which means the function will not save files
#'                 for you.
#' @param savePath character, Specifies the path to save your outputs, you do not need to
#'                 specify if \code{saveThem} is turned off. If missing, the current working
#'                 directory will be choosed.
#'
#' @return Depends on \code{saveThem}, if it hs turned off (i.e., \code{FALSE}). The
#'         function returns three datasets:
#'         \itemize{
#'         \item{\code{} contains the crews for conducting QA;}
#'         \item{\code{}}
#'         }
#'
#' @importFrom data.table ':=' data.table
#' @importFrom dplyr '%>%'
#' @importFrom ROracle dbConnect dbGetQuery dbDisconnect
#' @importFrom DBI dbDriver
#'
#' @rdname loadVGISSample
#' @author Yong Luo
loadVGISSample <- function(userName, passWord, saveThem = FALSE,
                           savePath = file.path(".")){
  drv <- dbDriver("Oracle")
  connect_string <- getServer(databaseName = "VGIS")
  con <- dbConnect(drv, username = userName, password = passWord,
                   dbname = connect_string)
  sampl <- dbGetQuery(con, "SELECT
                      vgis.PLOT_CLUSTERS.PLOT_CLUSTER_SKEY          as cl_key,
                      vgis.vgis_projects.PROJECT_SKEY               as proj_key,
                      vgis.MEASMT_VISITATIONS.MEASMT_VISIT_SKEY     as vst_key,
                      vgis.vgis_projects.PROJECT_BUSINESS_ID        as proj_id,
                      vgis.PLOT_CLUSTERS.SAMPLE_EXP_PLOT_NUM        AS SAMP_NO,
                      vgis.DESIGN_SPECIFICATIONS.BUSINESS_IDENTIFIER_VALUE as type_,
                      vgis.MEASMT_VISITATIONS.SAMPLE_INTENT         as intent,
                      vgis.MEASMT_VISITATIONS.MEASMT_VISITN_NUM     as visit,
                      vgis.MEASMT_VISITATIONS.MEASMT_DATE           AS Ms_DT,
                      vgis.MEASMT_VISITATIONS.create_DATE           AS or_DT,
                      vgis.MEASMT_VISITATIONS.POLYGON_IDENTIFIER    as poly_id
                      FROM
                      vgis.PLOT_CLUSTERS ,
                      vgis.vgis_projects,
                      vgis.PROJECT_ASSOCIATIONS,
                      vgis.MEASMT_VISITATIONS,
                      vgis.DESIGN_SPECIFICATIONS
                      WHERE
                      (vgis.PROJECT_ASSOCIATIONS.PRJ_PROJECT_SKEY
                      = vgis.vgis_projects.PROJECT_SKEY)
                      AND (vgis.PROJECT_ASSOCIATIONS.PCLTR_PLOT_CLUSTER_SKEY
                      = vgis.PLOT_CLUSTERS.PLOT_CLUSTER_SKEY)
                      AND (vgis.DESIGN_SPECIFICATIONS.DESIGN_SKEY
                      = vgis.PLOT_CLUSTERS.DES_DESIGN_SKEY)
                      AND (vgis.MEASMT_VISITATIONS.PCLTR_PLOT_CLUSTER_SKEY
                      = vgis.PLOT_CLUSTERS.PLOT_CLUSTER_SKEY)") %>%
    data.table
  dbDisconnect(con)
  set(sampl, , c("CL_KEY", "PROJ_KEY", "VST_KEY"), NULL)
  sampl[nchar(SAMP_NO) == 3, SAMP_NO := paste("0", SAMP_NO, sep = "")]
  sampl[nchar(SAMP_NO) == 2, SAMP_NO := paste("00", SAMP_NO, sep = "")]
  sampl[nchar(SAMP_NO) == 1, SAMP_NO := paste("000", SAMP_NO, sep = "")]
  sampl[, ':='(CLSTR_ID = getClusterID(projID = PROJ_ID,
                                       sampleNO = SAMP_NO,
                                       sampleType = TYPE_,
                                       intent = INTENT,
                                       visit = VISIT),
               TYPE_CD = getTypeCode(sampleType = TYPE_,
                                     intent = INTENT,
                                     visit = VISIT),
               MEAS_DT = as.Date(MS_DT),
               ORCL_DT = as.Date(OR_DT),
               LOAD_DT = as.Date(Sys.time()))]
  sampl[, cluster_time := length(MEAS_DT), by = "CLSTR_ID"]
  if(nrow(sampl[cluster_time > 1,]) > 0){
    stop("Duplicate cluster header records.")
  }
  sampl <- unique(sampl[,.(CLSTR_ID, PROJ_ID, SAMP_NO, TYPE_CD,
                           MEAS_DT, ORCL_DT, LOAD_DT)],
                  by = "CLSTR_ID")
  if(saveThem){
      saveRDS(sampl, file.path(savePath, "sample_card_vgis.rds"))
  } else if (!saveThem){
    return(sampl)
  } else{
    stop("saveThem must be logical.")
  }
}
