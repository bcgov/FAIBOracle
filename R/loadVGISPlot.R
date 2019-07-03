#' Load the plot header data from Oracle database
#'
#'
#' @description This function is to load the sample header data from VGIS Oracle database. And
#'              associated them with cluster.
#'
#' @param userName character, Specifies a valid user name in VGIS Oracle database.
#' @param password character, Specifies the password to the user name.
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
#' @rdname loadVGISPlot
#' @author Yong Luo
loadVGISPlot <- function(userName, password, saveThem = FALSE,
                         savePath = file.path(".")){
  drv <- dbDriver("Oracle")
  connect.string <-"(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)
  (HOST=nrk1-scan.bcgov)(PORT=1521))
  (CONNECT_DATA=(SERVER=DEDICATED)(SERVICE_NAME=ENVPROD1.NRS.BCGOV)))"
  con <- dbConnect(drv, username = userName, password = password,
                   dbname = connect.string)
  plots <- dbGetQuery(con, "SELECT
                      vgis.vgis_projects.PROJECT_SKEY                        AS PROJ_KEY,
                      vgis.vgis_projects.PROJECT_BUSINESS_ID                 AS PROJ_ID ,
                      vgis.PLOT_CLUSTERS.PLOT_CLUSTER_SKEY              AS CL_KEY ,
                      vgis.PLOT_CLUSTERS.SAMPLE_EXP_PLOT_NUM            AS SAMP_NO,
                      vgis.DESIGN_SPECIFICATIONS.BUSINESS_IDENTIFIER_VALUE    as TYPE_,
                      vgis.MEASMT_VISITATIONS.SAMPLE_INTENT             AS INTENT,
                      vgis.CLUSTER_ELEMENTS.PLOT_IDENTIFIER             AS PLOT_ID,
                      vgis.BIOMETRIC_DESIGN_SPECIFICATION.BD_SPEC_SKEY  AS SP_KEY,
                      vgis.MEASMT_VISITATIONS.MEASMT_VISIT_SKEY         AS VST_KEY,
                      vgis.MEASMT_VISITATIONS.MEASMT_VISITN_NUM         AS VISiT,
                      vgis.MEASMT_VISITATIONS.MEASMT_DATE               AS MS_DT,
                      vgis.SPATIAL_DELIMITERS.SPTLD_TYPE                AS sp_type,
                      vgis.SPATIAL_DELIMITERS.BASAL_AREA_FTR            AS V_BAF,
                      vgis.SPATIAL_DELIMITERS.RADIUS                    AS F_RAD,
                      vgis.POINT_LOCATION_VISITATIONS.CE_VISIT_SKEY     as ce_key ,
                      vgis.POINT_LOCATION_VISITATIONS.PLOT_TYPE_CD      as pl_type,
                      vgis.POINT_LOCATION_VISITATIONS.PLOT_QUALIFIER_CD as pl_qual ,
                      vgis.SUBPOPULATIONS.DYNAMIC_CD                    AS DY_CD,
                      vgis.SUBPOPULATIONS.POPLTN_ELMNT_CD               AS pop,
                      vgis.SUBPOPULATIONS.SUBP_TYPE                     as sub_pop

                      FROM vgis.MEASMT_VISITATIONS,
                      vgis.PLOT_CLUSTERS,
                      vgis.POINT_LOCATION_VISITATIONS,
                      vgis.vgis_projects,
                      vgis.PROJECT_ASSOCIATIONS,
                      vgis.CLUSTER_ELEMENTS,
                      vgis.BIOMETRIC_DESIGN_SPECIFICATION,
                      vgis.SPATIAL_DELIMITERS,
                      vgis.SUBPOPULATIONS,
                      vgis.DESIGN_SPECIFICATIONS

                      WHERE (vgis.MEASMT_VISITATIONS.PSTAT_PROCESS_STATE_CD='DDL'
                      AND vgis.SPATIAL_DELIMITERS.SPTLD_TYPE<>'LIN'
                      AND vgis.SUBPOPULATIONS.POPLTN_ELMNT_CD='TREE'
                      AND vgis.SUBPOPULATIONS.TREE_EXTANT_CD IS NULL)
                      AND  ((vgis.MEASMT_VISITATIONS.PCLTR_PLOT_CLUSTER_SKEY
                      =vgis.PLOT_CLUSTERS.PLOT_CLUSTER_SKEY)
                      AND (vgis.POINT_LOCATION_VISITATIONS.MEAVT_MEASMT_VISIT_SKEY
                      =vgis.MEASMT_VISITATIONS.MEASMT_VISIT_SKEY)
                      AND (vgis.PLOT_CLUSTERS.PLOT_CLUSTER_SKEY
                      =vgis.PROJECT_ASSOCIATIONS.PCLTR_PLOT_CLUSTER_SKEY)
                      AND (vgis.PROJECT_ASSOCIATIONS.PRJ_PROJECT_SKEY
                      =vgis.vgis_projects.PROJECT_SKEY)
                      AND (vgis.POINT_LOCATION_VISITATIONS.CELMT_CLUSTER_ELEMENT_SKEY
                      =vgis.CLUSTER_ELEMENTS.CLUSTER_ELEMENT_SKEY)
                      AND (vgis.CLUSTER_ELEMENTS.BDSPEC_BD_SPEC_SKEY
                      =vgis.BIOMETRIC_DESIGN_SPECIFICATION.BD_SPEC_SKEY)
                      AND (vgis.BIOMETRIC_DESIGN_SPECIFICATION.SPTLD_SPATIAL_DELIM_SKEY
                      =vgis.SPATIAL_DELIMITERS.SPATIAL_DELIM_SKEY)
                      AND ( vgis.BIOMETRIC_DESIGN_SPECIFICATION.SUBP_SUB_POP_SKEY
                      =vgis.SUBPOPULATIONS.SUB_POP_SKEY)
                      AND (vgis.PLOT_CLUSTERS.DES_DESIGN_SKEY
                      = vgis.DESIGN_SPECIFICATIONS.DESIGN_SKEY))

                      ORDER BY vgis.vgis_projects.PROJECT_BUSINESS_ID ASC,
                      vgis.PLOT_CLUSTERS.SAMPLE_EXP_PLOT_NUM ASC,
                      vgis.CLUSTER_ELEMENTS.PLOT_IDENTIFIER ASC") %>%
    data.table
  dbDisconnect(con)
  set(plots, , c("CL_KEY", "PROJ_KEY", "VST_KEY", "MS_DT", "SP_KEY",
                  "CE_KEY",  "DY_CD", "SUB_POP"), NULL)
  plots <- plots[PLOT_ID <= 5,]
  plots[,':='(CLSTR_ID = getClusterID(PROJ_ID, SAMP_NO, TYPE_,
                                       INTENT, VISIT),
              PLOT = getPlotCode(PLOT_ID))]
  set(plots, , c("PROJ_ID", "SAMP_NO", "TYPE_", "INTENT", "VISIT", "PLOT_ID"), NULL)
  plots <- plots[,.(CLSTR_ID, PLOT, V_BAF, F_RAD,
                    PL_QUAL, PL_TYPE, SP_TYPE, POP)]

  ## check the plot duplication
  plots[, plotlength := length(F_RAD), by = c("CLSTR_ID", "PLOT")]
  if(nrow(plots[plotlength > 1 & substr(CLSTR_ID, 1, 4) != "LGMW"]) > 0){
    warning("Plot information is duplicated. Please verify!")
  }
  plots <- unique(plots, by = c("CLSTR_ID", "PLOT"))
  plots[, plotlength := NULL]
  plots[!is.na(F_RAD) & PL_QUAL == "B", F_BDRY := TRUE]
  plots[!is.na(F_RAD) & PL_QUAL == "S", F_SPLT := TRUE]
  plots[!is.na(F_RAD) & PL_TYPE == "F", F_FULL := TRUE]
  plots[!is.na(F_RAD) & PL_TYPE == "H", F_HALF := TRUE]
  plots[!is.na(F_RAD) & PL_TYPE == "Q", F_QRTR := TRUE]

  plots[!is.na(V_BAF) & PL_QUAL == "B", V_BDRY := TRUE]
  plots[!is.na(V_BAF) & PL_QUAL == "S", V_SPLT := TRUE]
  plots[!is.na(V_BAF) & PL_TYPE == "F", V_FULL := TRUE]
  plots[!is.na(V_BAF) & PL_TYPE == "H", V_HALF := TRUE]
  plots[!is.na(V_BAF) & PL_TYPE == "Q", V_QRTR := TRUE]

  if(saveThem){
    saveRDS(plots, file.path(savePath, "plot_card_vgis.rds"))
  } else if (!saveThem){
    return(plots)
  } else{
    stop("saveThem must be logical.")
  }
}
