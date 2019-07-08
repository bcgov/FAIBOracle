#' Load the photo data from Oracle database
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
#' @rdname loadVGISPhoto
#' @author Yong Luo
loadVGISPhoto <- function(userName, passWord, saveThem = FALSE,
                          savePath = file.path(".")){
  drv <- dbDriver("Oracle")
  connect.string <-"(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)
  (HOST=nrk1-scan.bcgov)(PORT=1521))
  (CONNECT_DATA=(SERVER=DEDICATED)(SERVICE_NAME=ENVPROD1.NRS.BCGOV)))"
  con <- dbConnect(drv, username = userName, password = passWord,
                   dbname = connect.string)
  foto <- dbGetQuery(con, "SELECT
                     vgis.PLOT_CLUSTERS.PLOT_CLUSTER_SKEY                 AS CL_KEY,
                     vgis.vgis_projects.PROJECT_SKEY                           AS PROJ_KEY,
                     vgis.MEASMT_VISITATIONS.MEASMT_VISIT_SKEY            AS MV_KEY,
                     vgis.vgis_projects.PROJECT_BUSINESS_ID                    AS PROJ_ID,
                     vgis.PLOT_CLUSTERS.SAMPLE_EXP_PLOT_NUM               AS SAMP_NO,
                     vgis.DESIGN_SPECIFICATIONS.BUSINESS_IDENTIFIER_VALUE AS TYPE_,
                     vgis.MEASMT_VISITATIONS.SAMPLE_INTENT                AS INTENT,
                     vgis.MEASMT_VISITATIONS.MEASMT_VISITN_NUM            AS VISIT,
                     vgis.MEASMT_VISITATIONS.POLYGON_IDENTIFIER           AS POLYGON,
                     vgis.SAMPLE_PHOTOS.SUBJECT                           AS SUBJECT,
                     vgis.SAMPLE_PHOTOS.COLLECTION_IND                    AS COLL_IND,
                     vgis.SAMPLE_PHOTOS.DESCRIPTION                       AS DESCRIP
                     FROM
                     vgis.PLOT_CLUSTERS ,
                     vgis.vgis_projects,
                     vgis.PROJECT_ASSOCIATIONS,
                     vgis.MEASMT_VISITATIONS,
                     vgis.DESIGN_SPECIFICATIONS,
                     vgis.SAMPLE_PHOTOS
                     WHERE
                     (vgis.PROJECT_ASSOCIATIONS.PRJ_PROJECT_SKEY
                     = vgis.vgis_projects.PROJECT_SKEY)
                     AND (vgis.PROJECT_ASSOCIATIONS.PCLTR_PLOT_CLUSTER_SKEY
                     = vgis.PLOT_CLUSTERS.PLOT_CLUSTER_SKEY)
                     AND (vgis.DESIGN_SPECIFICATIONS.DESIGN_SKEY
                     = vgis.PLOT_CLUSTERS.DES_DESIGN_SKEY)
                     AND (vgis.MEASMT_VISITATIONS.PCLTR_PLOT_CLUSTER_SKEY
                     = vgis.PLOT_CLUSTERS.PLOT_CLUSTER_SKEY)
                     AND (vgis.SAMPLE_PHOTOS.MEAVT_MEASMT_VISIT_SKEY
                     = vgis.MEASMT_VISITATIONS.MEASMT_VISIT_SKEY)") %>%
    data.table
  dbDisconnect(con)
  set(foto, , c("CL_KEY", "PROJ_KEY", "MV_KEY"), NULL)
  foto[,':='(CLSTR_ID = getClusterID(PROJ_ID, SAMP_NO, TYPE_,
                                      INTENT, VISIT))]
  set(foto, , c("PROJ_ID", "SAMP_NO", "TYPE_", "INTENT", "VISIT"), NULL)
  if(saveThem){
    saveRDS(foto[,.(CLSTR_ID, POLYGON, SUBJECT, COLL_IND, DESCRIP)],
            file.path(savePath, "photo_card_vgis.rds"))
  } else if (!saveThem){
    return(foto[,.(CLSTR_ID, POLYGON, SUBJECT, COLL_IND, DESCRIP)])
  } else{
    stop("saveThem must be logical.")
  }
}
