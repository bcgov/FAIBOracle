#' Load the range data from Oracle database
#'
#'
#' @description This function is to load the crew data from VGIS Oracle database. And
#'              associated them with cluster.
#'
#' @param userName character, Specifies a valid user name in VGIS Oracle database.
#' @param password character, Specifies the password to the user name.
#' @param saveThem logical, Specifies whether the loaded data should be saved or returned.
#'                 The default value is FALSE, which means the function will not save files
#'                 for you.
#' @param savePath character, Specifies the path to save your outputs, you do not need to
#'                 specify if \code{saveThem} is turned off.
#'
#'
#' @return Depends on \code{saveThem}, if it hs turned off (i.e., \code{FALSE}). The
#'         function returns three datasets:
#'         \itemize{
#'         \item{\code{forage} }
#'         \item{\code{shrub}}
#'         }
#'
#' @importFrom data.table ':=' data.table
#' @importFrom dplyr '%>%'
#' @importFrom ROracle dbConnect dbGetQuery dbDisconnect
#' @importFrom DBI dbDriver
#'
#' @rdname loadVGISRange
#' @author Yong Luo
loadVGISRange <- function(userName, password,
                          saveThem = FALSE, savePath = "."){
  drv <- dbDriver("Oracle")
  connect.string <-"(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)
  (HOST=nrk1-scan.bcgov)(PORT=1521))
  (CONNECT_DATA=(SERVER=DEDICATED)(SERVICE_NAME=ENVPROD1.NRS.BCGOV)))"
  con <- dbConnect(drv, username = userName, password = password,
                   dbname = connect.string)
  c4forage <- dbGetQuery(con, "SELECT
      vgis.PLOT_CLUSTERS.PLOT_CLUSTER_SKEY                        AS CL_KEY,
                         vgis.vgis_projects.PROJECT_SKEY                                  AS PROJ_KEY,
                         vgis.vgis_projects.PROJECT_BUSINESS_ID                           AS PROJ_ID,
                         vgis.PLOT_CLUSTERS.SAMPLE_EXP_PLOT_NUM                      AS SAMP_NO,
                         vgis.DESIGN_SPECIFICATIONS.BUSINESS_IDENTIFIER_VALUE        AS TYPE_,
                         vgis.MEASMT_VISITATIONS.SAMPLE_INTENT                       AS INTENT,
                         vgis.MEASMT_VISITATIONS.MEASMT_VISITN_NUM                   AS VISIT,
                         vgis.FORAGE_PRODUCTION_MEASMTS.GRAMINOIDS_COLLECTED_IND     AS GRAM_Q,
                         vgis.FORAGE_PRODUCTION_MEASMTS.FORBS_COLLECTED_IND         AS FORBS_Q,
                         vgis.FORAGE_PRODUCTION_MEASMTS.GRAMINOIDS_DRY_WEIGHT       AS MS_GRAM ,
                         vgis.FORAGE_PRODUCTION_MEASMTS.FORBS_DRY_WEIGHT            AS MS_FORB,
                         vgis.FORAGE_PRODUCTION_MEASMTS.FORAGE_MEASMT_SKEY          AS MS_KEY,
                         vgis.FORAGE_PRODUCTION_MEASMTS.observed_plot_fraction      as Plot_siz,
                         vgis.POINT_LOCATION_VISITATIONS.LENGTH_OBSERVED            AS X,
                         vgis.FORAGE_PRODUCTION_MEASMTS.CEV_CE_VISIT_SKEY           AS VST_KEY,
                         vgis.POINT_LOCATION_VISITATIONS.CE_VISIT_SKEY              AS CVST_KEY,
                         vgis.FORAGE_UTILZATION_ASSMTS.PLOT_NUMBER                  AS PLOT_NO,
                         vgis.FORAGE_UTILIZATION_CDS.FORAGE_UTILZN_CLASS_CD         AS UTIL_CD

                         FROM vgis.MEASMT_VISITATIONS,
                         vgis.PLOT_CLUSTERS,
                         vgis.POINT_LOCATION_VISITATIONS,
                         vgis.vgis_projects,
                         vgis.PROJECT_ASSOCIATIONS,
                         vgis.FORAGE_PRODUCTION_MEASMTS,
                         vgis.FORAGE_UTILZATION_ASSMTS,
                         vgis.FORAGE_UTILIZATION_CDS,
                         vgis.DESIGN_SPECIFICATIONS

                         WHERE vgis.MEASMT_VISITATIONS.PSTAT_PROCESS_STATE_CD='DDL'
                         AND  ((vgis.MEASMT_VISITATIONS.PCLTR_PLOT_CLUSTER_SKEY
                         =vgis.PLOT_CLUSTERS.PLOT_CLUSTER_SKEY)
                         AND (vgis.POINT_LOCATION_VISITATIONS.MEAVT_MEASMT_VISIT_SKEY
                         =vgis.MEASMT_VISITATIONS.MEASMT_VISIT_SKEY)
                         AND (vgis.PLOT_CLUSTERS.PLOT_CLUSTER_SKEY
                         =vgis.PROJECT_ASSOCIATIONS.PCLTR_PLOT_CLUSTER_SKEY)
                         AND (PROJECT_ASSOCIATIONS.PRJ_PROJECT_SKEY
                         =vgis.vgis_projects.PROJECT_SKEY)
                         AND (vgis.FORAGE_PRODUCTION_MEASMTS.CEV_CE_VISIT_SKEY
                         =vgis.POINT_LOCATION_VISITATIONS.CE_VISIT_SKEY)
                         AND (vgis.FORAGE_UTILZATION_ASSMTS.FPM_FORAGE_MEASMT_SKEY
                         =vgis.FORAGE_PRODUCTION_MEASMTS.FORAGE_MEASMT_SKEY)
                         AND (vgis.FORAGE_UTILZATION_ASSMTS.FUC_FORAGE_UTILZN_CLASS_CD
                         =vgis.FORAGE_UTILIZATION_CDS.FORAGE_UTILZN_CLASS_CD)
                         AND (vgis.PLOT_CLUSTERS.DES_DESIGN_SKEY=
                         vgis.DESIGN_SPECIFICATIONS.DESIGN_SKEY))

                         ORDER BY vgis.PLOT_CLUSTERS.SAMPLE_EXP_PLOT_NUM ASC") %>%
    data.table
  set(c4forage, , c("CL_KEY", "PROJ_KEY", "VST_KEY",
                    "CVST_KEY", "MS_KEY"), NULL)
  c4forage[, ':='(CLSTR_ID = getClusterID(projID = PROJ_ID,
                                          sampleNO = SAMP_NO,
                                          sampleType = TYPE_,
                                          intent = INTENT,
                                          visit = VISIT))]
  set(c4forage, , c("PROJ_ID", "SAMP_NO", "TYPE_",
                    "INTENT", "VISIT"), NULL)
  c4forage[GRAM_Q == "N", MS_GRAM := 0]
  c4forage[FORBS_Q == "N", MS_FORB := 0]

  c4forage[UTIL_CD == 0, UTIL_VAL := 0]
  c4forage[UTIL_CD == 1, UTIL_VAL := 7.5]
  c4forage[UTIL_CD == 2, UTIL_VAL := 26]
  c4forage[UTIL_CD == 3, UTIL_VAL := 46]
  c4forage[UTIL_CD == 4, UTIL_VAL := 74]
  c4forage[UTIL_CD == 5, UTIL_VAL := 90]

  c4forage[, ':='(NO_PLOTS = length(UTIL_CD[!is.na(UTIL_CD)]),
                  UTIL_AVG = mean(UTIL_VAL, na.rm = TRUE)),
           by = c("CLSTR_ID")]
  c4forage[, UTIL_CD := NULL]

  c4forage <- reshape(c4forage, v.names = "UTIL_VAL",
                      timevar = "PLOT_NO", idvar = c("CLSTR_ID"),
                      direction = "wide")
  c4forage <- c4forage[,.(CLSTR_ID, GRAM_Q, MS_GRAM, FORBS_Q, MS_FORB,
                          PLOT_SIZE = PLOT_SIZ,
                          UTIL_CD1 = UTIL_VAL.1,
                          UTIL_CD2 = UTIL_VAL.2,
                          UTIL_CD3 = UTIL_VAL.3,
                          UTIL_CD4 = UTIL_VAL.4,
                          UTIL_AVG, NO_PLOTS)]



  c4shrub <- dbGetQuery(con, "SELECT
    vgis.PLOT_CLUSTERS.PLOT_CLUSTER_SKEY                      AS CL_KEY,
                        vgis.vgis_projects.PROJECT_SKEY                                AS PROJ_KEY,
                        vgis.vgis_projects.PROJECT_BUSINESS_ID                         AS PROJ_ID,
                        vgis.PLOT_CLUSTERS.SAMPLE_EXP_PLOT_NUM                    AS SAMP_NO,
                        vgis.DESIGN_SPECIFICATIONS.BUSINESS_IDENTIFIER_VALUE      AS TYPE_,
                        vgis.MEASMT_VISITATIONS.SAMPLE_INTENT                     AS INTENT,
                        vgis.MEASMT_VISITATIONS.MEASMT_VISITN_NUM                 AS VISIT,
                        vgis.MEASMT_VISITATIONS.MEASMT_VISIT_SKEY                 AS MVST_KEY,
                        vgis.MEASMT_VISITATIONS.MEASMT_DATE                       AS MS_DT,
                        vgis.POINT_LOCATION_VISITATIONS.CE_VISIT_SKEY             AS VST_KEY,
                        vgis.POINT_LOCATION_VISITATIONS.LENGTH_OBSERVED           AS OBS_LLEN,
                        vgis.CLUSTER_ELEMENTS.PLOT_IDENTIFIER                     AS TRANSECT,
                        vgis.RANGE_VEGETATION_MEASMTS.RANGE_MEASMT_SKEY           AS RNG_KEY,
                        vgis.RANGE_VEGETATION_MEASMTS.ITEM_NUM                    AS ITEM_NO,
                        vgis.RANGE_VEGETATION_MEASMTS.VLC_VEGETATION_LAYER_CD     AS LAYER,
                        vgis.TAXONOMIC_NAMES.LONG_CD                              AS SPECIES,
                        vgis.RANGE_VEGETATION_MEASMTS.PHENOLOGY_IDENTIFIER        AS PHENOLOG,
                        vgis.TRANSECT_COVERAGES.RVM_RANGE_MEASMT_SKEY             AS MS_KEY,
                        vgis.TRANSECT_COVERAGES.TRANSECT_COVERAGE_SKEY            AS COV_KEY,
                        vgis.TRANSECT_COVERAGES.HORIZONTAL_DISTANCE               AS LINE_LEN

                        FROM vgis.MEASMT_VISITATIONS,
                        vgis.PLOT_CLUSTERS,
                        vgis.vgis_projects,
                        vgis.PROJECT_ASSOCIATIONS,
                        vgis.POINT_LOCATION_VISITATIONS,
                        vgis.CLUSTER_ELEMENTS,
                        vgis.TAXONOMIC_NAMES,
                        vgis.RANGE_VEGETATION_MEASMTS,
                        vgis.TRANSECT_COVERAGES,
                        vgis.DESIGN_SPECIFICATIONS,
                        vgis.BIOMETRIC_DESIGN_SPECIFICATION,
                        vgis.GROUP_TAXON_MEMBERSHIPS

                        WHERE (vgis.MEASMT_VISITATIONS.PSTAT_PROCESS_STATE_CD='DDL' AND
                        vgis.BIOMETRIC_DESIGN_SPECIFICATION.BD_SPEC_SKEY = 1 )
                        AND  ( (vgis.MEASMT_VISITATIONS.PCLTR_PLOT_CLUSTER_SKEY
                        = vgis.PLOT_CLUSTERS.PLOT_CLUSTER_SKEY)
                        AND (vgis.PLOT_CLUSTERS.PLOT_CLUSTER_SKEY
                        = vgis.PROJECT_ASSOCIATIONS.PCLTR_PLOT_CLUSTER_SKEY)
                        AND (vgis.PROJECT_ASSOCIATIONS.PRJ_PROJECT_SKEY
                        = vgis.vgis_projects.PROJECT_SKEY)
                        AND (vgis.POINT_LOCATION_VISITATIONS.MEAVT_MEASMT_VISIT_SKEY
                        = vgis.MEASMT_VISITATIONS.MEASMT_VISIT_SKEY)
                        AND (vgis.POINT_LOCATION_VISITATIONS.CELMT_CLUSTER_ELEMENT_SKEY
                        = vgis.CLUSTER_ELEMENTS.CLUSTER_ELEMENT_SKEY)
                        AND (vgis.BIOMETRIC_DESIGN_SPECIFICATION.BD_SPEC_SKEY
                        =vgis.CLUSTER_ELEMENTS.BDSPEC_BD_SPEC_SKEY)
                        AND (vgis.POINT_LOCATION_VISITATIONS.CE_VISIT_SKEY
                        = vgis.RANGE_VEGETATION_MEASMTS.CEV_CE_VISIT_SKEY(+))
                        AND ( vgis.RANGE_VEGETATION_MEASMTS.GTO_GROUP_TAXON_SKEY
                        = vgis.GROUP_TAXON_MEMBERSHIPS.GROUP_TAXON_SKEY(+))
                        AND (vgis.GROUP_TAXON_MEMBERSHIPS.TAXC_TAXONOMIC_CD_SKEY
                        = vgis.TAXONOMIC_NAMES.TAXONOMIC_CD_SKEY(+))
                        AND (vgis.TRANSECT_COVERAGES.RVM_RANGE_MEASMT_SKEY(+)
                        = vgis.RANGE_VEGETATION_MEASMTS.RANGE_MEASMT_SKEY)
                        AND (vgis.PLOT_CLUSTERS.DES_DESIGN_SKEY
                        = vgis.DESIGN_SPECIFICATIONS.DESIGN_SKEY))") %>%
    data.table
  dbDisconnect(con)
  set(c4shrub, , c("CL_KEY", "PROJ_KEY", "VST_KEY", "MVST_KEY",
                   "MS_DT", "RNG_KEY", "MS_KEY", "COV_KEY"), NULL)
  c4shrub[,':='(CLSTR_ID = getClusterID(PROJ_ID, SAMP_NO, TYPE_,
                                        INTENT, VISIT),
                ITEM_NO = gsub(" ", "", ITEM_NO))]
  set(c4shrub, , c("PROJ_ID", "SAMP_NO", "TYPE_", "INTENT",
                   "VISIT"), NULL)
  c4shrub[nchar(ITEM_NO) == 1, ITEM_NO := paste("00", ITEM_NO, sep = "")]
  c4shrub[nchar(ITEM_NO) == 2, ITEM_NO := paste("0", ITEM_NO, sep = "")]
  c4shrub <- c4shrub[!is.na(ITEM_NO),.(CLSTR_ID, TRANSECT, ITEM_NO,
                                       OBS_LLEN, LAYER, SPECIES,
                                       PHENOLOG, COVERAGE = LINE_LEN)]
  c4shrub[,':='(NUM_COV = length(SPECIES),
                TOT_COV = sum(COVERAGE)),
          by = c("CLSTR_ID", "TRANSECT", "ITEM_NO")]

  if(saveThem){
    saveRDS(list(forage = c4forage,
                 shrub = c4shrub),
            file.path(savePath, "range_card_vgis.rds"))
  } else if (!saveThem){
    return(list(forage = c4forage,
                shrub = c4shrub))
  } else {
    stop("saveThem must be logical.")
  }
}
