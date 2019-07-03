#' Load the CWD data from Oracle database
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
#' @rdname loadVGISCWD
#' @author Yong Luo
loadVGISCWD <- function(userName, password, saveThem = FALSE,
                         savePath = file.path(".")){
  drv <- dbDriver("Oracle")
  connect.string <-"(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)
  (HOST=nrk1-scan.bcgov)(PORT=1521))
  (CONNECT_DATA=(SERVER=DEDICATED)(SERVICE_NAME=ENVPROD1.NRS.BCGOV)))"
  con <- dbConnect(drv, username = userName, password = password,
                   dbname = connect.string)
  c6cwd <- dbGetQuery(con, "SELECT
        vgis.PLOT_CLUSTERS.PLOT_CLUSTER_SKEY AS CL_KEY,
                      vgis.vgis_projects.PROJECT_SKEY AS PROJ_KEY,
                      vgis.vgis_projects.PROJECT_BUSINESS_ID AS PROJ_ID,
                      vgis.PLOT_CLUSTERS.SAMPLE_EXP_PLOT_NUM AS SAMP_NO,
                      vgis.DESIGN_SPECIFICATIONS.BUSINESS_IDENTIFIER_VALUE AS TYPE_,
                      vgis.MEASMT_VISITATIONS.SAMPLE_INTENT AS INTENT,
                      vgis.MEASMT_VISITATIONS.MEASMT_VISITN_NUM AS VISIT,
                      vgis.MEASMT_VISITATIONS.MEASMT_VISIT_SKEY AS MVST_KEY,
                      vgis.CLUSTER_ELEMENTS.PLOT_IDENTIFIER as TRANSECT,
                      vgis.CLUSTER_ELEMENTS.AZIMUTH_BEARING as AZIMUTH,
                      vgis.POINT_LOCATION_VISITATIONS.LENGTH_OBSERVED as OBS_LlEN,
                      vgis.COARSE_WOODY_DEBRIS_MEASMTS.CWDM_TYPE as cwd_type,
                      vgis.COARSE_WOODY_DEBRIS_MEASMTS.SOURCE_NUM as item_no,
                      vgis.COARSE_WOODY_DEBRIS_MEASMTS.DECAY_CLASS_CD as dec_CLS,
                      vgis.COARSE_WOODY_DEBRIS_MEASMTS.LENGTH_GREATER_THAN_3M_IND as merch,
                      vgis.COARSE_WOODY_DEBRIS_MEASMTS.DIAMETER as  diameter,
                      vgis.COARSE_WOODY_DEBRIS_MEASMTS.PIECE_LENGTH as length,
                      vgis.COARSE_WOODY_DEBRIS_MEASMTS.TRANSECT_HORIZN_LENGTH as alength,
                      vgis.COARSE_WOODY_DEBRIS_MEASMTS.TILT_ANGLE as angle,
                      vgis.COARSE_WOODY_DEBRIS_MEASMTS.TRANSECT_VERTICAL_DEPTH as adepth,
                      vgis.COARSE_WOODY_DEBRIS_MEASMTS.LIKELY_TO_BE_REMOVED_IND as remove,
                      vgis.TAXONOMIC_NAMES.LONG_CD as LONG_SPECIES,
                      vgis.CWD_DECAY_ASSMTS.DECAY_CLASS_CD as class_x ,
                      vgis.CWD_DECAY_ASSMTS.PERCENT_OBSERVED as PCt
                      FROM vgis.MEASMT_VISITATIONS,
                      vgis.PLOT_CLUSTERS,
                      vgis.POINT_LOCATION_VISITATIONS,
                      vgis.vgis_projects,
                      vgis.PROJECT_ASSOCIATIONS,
                      vgis.CLUSTER_ELEMENTS,
                      vgis.COARSE_WOODY_DEBRIS_MEASMTS,
                      vgis.TAXONOMIC_NAMES,
                      vgis.CWD_DECAY_ASSMTS,
                      vgis.GROUP_TAXON_MEMBERSHIPS,
                      vgis.BIOMETRIC_DESIGN_SPECIFICATION,
                      vgis.DESIGN_SPECIFICATIONS
                      WHERE (vgis.MEASMT_VISITATIONS.PSTAT_PROCESS_STATE_CD ='DDL')
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
                      AND (vgis.BIOMETRIC_DESIGN_SPECIFICATION.BD_SPEC_SKEY
                      =vgis.CLUSTER_ELEMENTS.BDSPEC_BD_SPEC_SKEY)
                      AND (vgis.POINT_LOCATION_VISITATIONS.CE_VISIT_SKEY
                      =vgis.COARSE_WOODY_DEBRIS_MEASMTS.CEV_CE_VISIT_SKEY(+))
                      AND (vgis.COARSE_WOODY_DEBRIS_MEASMTS.GTO_GROUP_TAXON_SKEY
                      =vgis.GROUP_TAXON_MEMBERSHIPS.GROUP_TAXON_SKEY(+))
                      AND (vgis.CWD_DECAY_ASSMTS.CWDM_CWD_SKEY(+)
                      =vgis.COARSE_WOODY_DEBRIS_MEASMTS.CWD_SKEY)
                      AND (vgis.GROUP_TAXON_MEMBERSHIPS.TAXC_TAXONOMIC_CD_SKEY
                      =vgis.TAXONOMIC_NAMES.TAXONOMIC_CD_SKEY(+))
                      AND (vgis.PLOT_CLUSTERS.DES_DESIGN_SKEY
                      =vgis.DESIGN_SPECIFICATIONS.DESIGN_SKEY))
                      AND (vgis.POINT_LOCATION_VISITATIONS.LENGTH_OBSERVED IS NOT NULL )") %>%
    data.table
  dbDisconnect(con)
  set(c6cwd, , c("CL_KEY", "PROJ_KEY", "MVST_KEY"), NULL)
  c6cwd[,':='(CLSTR_ID = getClusterID(PROJ_ID, SAMP_NO, TYPE_,
                                      INTENT, VISIT))]
  set(c6cwd, , c("PROJ_ID", "SAMP_NO", "TYPE_",
                 "INTENT", "VISIT"), NULL)
  c6cwd <- c6cwd[!is.na(ITEM_NO),]
  c6cwd[, ITEM_NO := gsub(" ", "", ITEM_NO)]
  c6cwd[nchar(ITEM_NO) == 1, ITEM_NO := paste("00", ITEM_NO, sep = "")]
  c6cwd[nchar(ITEM_NO) == 2, ITEM_NO := paste("0", ITEM_NO, sep = "")]
  c6cwd[CWD_TYPE == "CA1", ':='(DIAMETER = NA,
                                ANGLE = NA)]
  c6cwd[CLASS_X == 1, DATA_TYPE := "PERCENTAGE"]
  c6cwd[CLASS_X != 1 | is.na(CLASS_X),
        ':='(DATA_TYPE = "CLASSOTH",
             CLS_OTH = CLASS_X)]
  ## KEEP LONG FORM TO MAKE SURE THE INFOMATION IS NOT LOST.
  c6cwd <- c6cwd[,.(CLSTR_ID, TRANSECT, ITEM_NO, AZIMUTH, OBS_LLEN, DEC_CLS,
                    MERCH, DIAMETER, LENGTH, ALENGTH, ANGLE, ADEPTH, REMOVE,
                    LONG_SPECIES, DATA_TYPE, PCT_CLS1 = PCT, CLS_OTH)]
  if(saveThem){
    saveRDS(c6cwd, file.path(savePath, "CWD_card_vgis.rds"))
  } else if (!saveThem){
    return(c6cwd)
  } else{
    stop("saveThem must be logical.")
  }
}
