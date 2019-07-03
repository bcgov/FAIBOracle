#' Load the stump and small tree data from Oracle database
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
#' @rdname loadVGISStumpSTree
#' @author Yong Luo
loadVGISStumpSTree <- function(userName, password, saveThem = FALSE,
                               savePath = file.path(".")){
  drv <- dbDriver("Oracle")
  connect.string <-"(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)
  (HOST=nrk1-scan.bcgov)(PORT=1521))
  (CONNECT_DATA=(SERVER=DEDICATED)(SERVICE_NAME=ENVPROD1.NRS.BCGOV)))"
  con <- dbConnect(drv, username = userName, password = password,
                   dbname = connect.string)
  c10stmp <- dbGetQuery(con, "SELECT
                        vgis.PLOT_CLUSTERS.PLOT_CLUSTER_SKEY        AS CL_KEY,
                        vgis.vgis_projects.PROJECT_SKEY                  AS PROJ_KEY,
                        vgis.vgis_projects.PROJECT_BUSINESS_ID           AS PROJ_ID,
                        vgis.PLOT_CLUSTERS.SAMPLE_EXP_PLOT_NUM      AS SAMP_NO,
                        vgis.DESIGN_SPECIFICATIONS.BUSINESS_IDENTIFIER_VALUE AS TYPE_,
                        vgis.MEASMT_VISITATIONS.SAMPLE_INTENT       AS INTENT,
                        vgis.MEASMT_VISITATIONS.MEASMT_VISITN_NUM   AS VISIT,
                        vgis.MEASMT_VISITATIONS.MEASMT_VISIT_SKEY   AS MVST_KEY,
                        vgis.CLUSTER_ELEMENTS.PLOT_IDENTIFIER       AS PLOT_ID,
                        vgis.POINT_LOCATION_VISITATIONS.PLOT_TYPE_CD AS PL_TYPE,
                        vgis.POINT_LOCATION_VISITATIONS.PLOT_QUALIFIER_CD AS PL_QUAL,
                        vgis.SPATIAL_DELIMITERS.RADIUS              AS F_RAD,
                        vgis.SPATIAL_DELIMITERS.SPTLD_TYPE          AS X,
                        vgis.TAXONOMIC_NAMES.LONG_CD               AS LONG_SPECIES,
                        vgis.TREE_MEASMTS.FREQUENCY_COUNT           AS FREQ,
                        vgis.DIAM_ASSMTS.DIAMETER                   AS Dib,
                        vgis.HT_ASSMTS.HEIGHT                       AS HEIGHT,
                        vgis.TREE_MEASMTS.SOUND_WOOD_PERCENT        AS PCT_SND,
                        vgis.TREE_WILDLIFE_USE_OCCURRENCES.TWUC_WILDLIFE_USE_CD     AS WL_USE,
                        TREE_WILDLIFE_FTR_OCCURRENC_A1.TWFC_FEATURE_STATE_NUM      AS WL_WOOD ,
                        vgis.TREE_WILDLIFE_FTR_OCCURRENCES.TWFC_FEATURE_STATE_NUM   AS BARK_RET,
                        vgis.DAMAGE_AGENT_VEG_SAM_VERSIONS.DAMAGE_AGENT_CD          AS ROOT_ROT
                        FROM
                        vgis.PLOT_CLUSTERS,
                        vgis.MEASMT_VISITATIONS,
                        vgis.CLUSTER_ELEMENTS,
                        vgis.POINT_LOCATION_VISITATIONS,
                        vgis.SPATIAL_DELIMITERS,
                        vgis.BIOMETRIC_DESIGN_SPECIFICATION,
                        vgis.TREE_MEASMTS,
                        vgis.TREES,
                        vgis.GROUP_TAXON_MEMBERSHIPS,
                        vgis.TAXONOMIC_NAMES,
                        vgis.DAMAGE_AGENT_VEG_SAM_VERSIONS,
                        vgis.vgis_projects,
                        vgis.PROJECT_ASSOCIATIONS,
                        vgis.DIAM_ASSMTS,
                        vgis.HT_ASSMTS,
                        vgis.DAMAGE_OCCURRENCES,
                        vgis.TREE_WILDLIFE_USE_OCCURRENCES,
                        vgis.TREE_WILDLIFE_FTR_OCCURRENCES TREE_WILDLIFE_FTR_OCCURRENC_A1,
                        vgis.TREE_WILDLIFE_FTR_OCCURRENCES,
                        vgis.DESIGN_SPECIFICATIONS

                        WHERE(vgis.MEASMT_VISITATIONS.PSTAT_PROCESS_STATE_CD='DDL'
                        AND vgis.TREE_MEASMTS.STUMP_IND='Y'
                        AND vgis.TREES.TREE_TYPE='AGGR'
                        AND vgis.TREE_WILDLIFE_FTR_OCCURRENCES.TWFC_FEATURE_CATEGORY_CD='Bark'
                        AND TREE_WILDLIFE_FTR_OCCURRENC_A1.TWFC_FEATURE_CATEGORY_CD='Wood')
                        AND  ((vgis.MEASMT_VISITATIONS.PCLTR_PLOT_CLUSTER_SKEY
                        = vgis.PLOT_CLUSTERS.PLOT_CLUSTER_SKEY)
                        AND (vgis.POINT_LOCATION_VISITATIONS.MEAVT_MEASMT_VISIT_SKEY
                        = vgis.MEASMT_VISITATIONS.MEASMT_VISIT_SKEY)
                        AND (vgis.POINT_LOCATION_VISITATIONS.CELMT_CLUSTER_ELEMENT_SKEY
                        = vgis.CLUSTER_ELEMENTS.CLUSTER_ELEMENT_SKEY)
                        AND (vgis.CLUSTER_ELEMENTS.BDSPEC_BD_SPEC_SKEY
                        = vgis.BIOMETRIC_DESIGN_SPECIFICATION.BD_SPEC_SKEY)
                        AND (vgis.BIOMETRIC_DESIGN_SPECIFICATION.SPTLD_SPATIAL_DELIM_SKEY
                        = vgis.SPATIAL_DELIMITERS.SPATIAL_DELIM_SKEY)
                        AND (vgis.TREE_MEASMTS.CEV_CE_VISIT_SKEY
                        = vgis.POINT_LOCATION_VISITATIONS.CE_VISIT_SKEY)
                        AND (vgis.TREES.TREE_SKEY = vgis.TREE_MEASMTS.TREE_TREE_SKEY)
                        AND (vgis.TREES.GTO_GROUP_TAXON_SKEY
                        = vgis.GROUP_TAXON_MEMBERSHIPS.GROUP_TAXON_SKEY)
                        AND (vgis.GROUP_TAXON_MEMBERSHIPS.TAXC_TAXONOMIC_CD_SKEY
                        = vgis.TAXONOMIC_NAMES.TAXONOMIC_CD_SKEY)
                        AND (vgis.PROJECT_ASSOCIATIONS.PRJ_PROJECT_SKEY
                        = vgis.vgis_projects.PROJECT_SKEY)
                        AND (vgis.PROJECT_ASSOCIATIONS.PCLTR_PLOT_CLUSTER_SKEY
                        = vgis.PLOT_CLUSTERS.PLOT_CLUSTER_SKEY)
                        AND(vgis.DIAM_ASSMTS.TRMEA_TREE_MEASMT_SKEY
                        =vgis.TREE_MEASMTS.TREE_MEASMT_SKEY)
                        AND(vgis.HT_ASSMTS.TRMEA_TREE_MEASMT_SKEY
                        =vgis.TREE_MEASMTS.TREE_MEASMT_SKEY)
                        AND (vgis.DAMAGE_OCCURRENCES.TRMEA_TREE_MEASMT_SKEY(+)
                        = vgis.TREE_MEASMTS.TREE_MEASMT_SKEY)
                        AND (vgis.DAMAGE_OCCURRENCES.DAGNC_DAVSV_SKEY
                        = vgis.DAMAGE_AGENT_VEG_SAM_VERSIONS.DAVSV_SKEY(+))
                        AND (vgis.TREE_WILDLIFE_USE_OCCURRENCES.TRMEA_TREE_MEASMT_SKEY (+)
                        = vgis.TREE_MEASMTS.TREE_MEASMT_SKEY)
                        AND (TREE_WILDLIFE_FTR_OCCURRENC_A1.TRMEA_TREE_MEASMT_SKEY (+)
                        = vgis.TREE_MEASMTS.TREE_MEASMT_SKEY)
                        AND (vgis.TREE_WILDLIFE_FTR_OCCURRENCES.TRMEA_TREE_MEASMT_SKEY (+)
                        = vgis.TREE_MEASMTS.TREE_MEASMT_SKEY)
                        AND (vgis.PLOT_CLUSTERS.DES_DESIGN_SKEY=
                        vgis.DESIGN_SPECIFICATIONS.DESIGN_SKEY))") %>%
    data.table
  set(c10stmp, , c("CL_KEY", "PROJ_KEY", "MVST_KEY"), NULL)
  c10stmp[,':='(CLSTR_ID = getClusterID(PROJ_ID, SAMP_NO, TYPE_,
                                        INTENT, VISIT),
                PLOT = getPlotCode(PLOT_ID),
                LONG_SPECIES = toupper(LONG_SPECIES))]
  set(c10stmp, , c("PROJ_ID", "SAMP_NO", "TYPE_", "INTENT", "VISIT", "PLOT_ID"), NULL)
  cardg <- c10stmp[,.(CLSTR_ID, PLOT, LONG_SPECIES, FREQ, DIB, HEIGHT,
                      PCT_SND, WL_USE, WL_WOOD, BARK_RET, ROOT_ROT)]

  cardeg <- c10stmp[,.(CLSTR_ID, PLOT, PL_TYPE, PL_QUAL, F_RAD, PL_ORIG = "STUMP")]
  cardeg <- cardeg[!duplicated(cardeg),]
  rm(c10stmp)

  c10stre <- dbGetQuery(con, "SELECT
                        vgis.PLOT_CLUSTERS.PLOT_CLUSTER_SKEY                 AS CL_KEY,
                        vgis.vgis_projects.PROJECT_SKEY                           AS PROJ_KEY,
                        vgis.vgis_projects.PROJECT_BUSINESS_ID                    AS PROJ_ID,
                        vgis.PLOT_CLUSTERS.SAMPLE_EXP_PLOT_NUM               AS SAMP_NO,
                        vgis.DESIGN_SPECIFICATIONS.BUSINESS_IDENTIFIER_VALUE AS TYPE_,
                        vgis.MEASMT_VISITATIONS.SAMPLE_INTENT                AS INTENT,
                        vgis.MEASMT_VISITATIONS.MEASMT_VISITN_NUM            AS VISIT,
                        vgis.MEASMT_VISITATIONS.MEASMT_VISIT_SKEY            AS MVST_KEY,
                        vgis.MEASMT_VISITATIONS.MEASMT_DATE                  AS MS_DT,
                        vgis.CLUSTER_ELEMENTS.PLOT_IDENTIFIER                AS PLOT_ID,
                        vgis.POINT_LOCATION_VISITATIONS.PLOT_TYPE_CD         AS PL_TYPE,
                        vgis.POINT_LOCATION_VISITATIONS.PLOT_QUALIFIER_CD    AS PL_QUAL,
                        vgis.SPATIAL_DELIMITERS.SPTLD_TYPE                   AS X,
                        vgis.SPATIAL_DELIMITERS.RADIUS                       AS F_RAD,
                        vgis.TAXONOMIC_NAMES.LONG_CD                        AS LONG_SPECIES,
                        vgis.FREQUENCY_COUNTS.TREE_COUNT                     AS TOTAL,
                        vgis.INTERVAL_CLASSES.LO_BND                         AS LO_BND,
                        vgis.INTERVAL_CLASSES.UP_BND                         AS UP_BND

                        FROM  vgis.MEASMT_VISITATIONS,
                        vgis.PLOT_CLUSTERS,
                        vgis.POINT_LOCATION_VISITATIONS,
                        vgis.vgis_projects,
                        vgis.PROJECT_ASSOCIATIONS,
                        vgis.CLUSTER_ELEMENTS,
                        vgis.BIOMETRIC_DESIGN_SPECIFICATION,
                        vgis.SPATIAL_DELIMITERS,
                        vgis.FREQUENCY_COUNTS,
                        vgis.TAXONOMIC_NAMES,
                        vgis.GROUP_TAXON_MEMBERSHIPS,
                        vgis.INTERVAL_CLASSES,
                        vgis.DESIGN_SPECIFICATIONS

                        WHERE (vgis.MEASMT_VISITATIONS.PSTAT_PROCESS_STATE_CD ='DDL')
                        AND  ((vgis.MEASMT_VISITATIONS.PCLTR_PLOT_CLUSTER_SKEY=
                        vgis.PLOT_CLUSTERS.PLOT_CLUSTER_SKEY)
                        AND (vgis.POINT_LOCATION_VISITATIONS.MEAVT_MEASMT_VISIT_SKEY=
                        vgis.MEASMT_VISITATIONS.MEASMT_VISIT_SKEY)
                        AND (vgis.PLOT_CLUSTERS.PLOT_CLUSTER_SKEY=
                        vgis.PROJECT_ASSOCIATIONS.PCLTR_PLOT_CLUSTER_SKEY)
                        AND (vgis.POINT_LOCATION_VISITATIONS.CELMT_CLUSTER_ELEMENT_SKEY=
                        vgis.CLUSTER_ELEMENTS.CLUSTER_ELEMENT_SKEY)
                        AND (vgis.CLUSTER_ELEMENTS.BDSPEC_BD_SPEC_SKEY=
                        vgis.BIOMETRIC_DESIGN_SPECIFICATION.BD_SPEC_SKEY)
                        AND (vgis.BIOMETRIC_DESIGN_SPECIFICATION.SPTLD_SPATIAL_DELIM_SKEY=
                        vgis.SPATIAL_DELIMITERS.SPATIAL_DELIM_SKEY)
                        AND (vgis.PROJECT_ASSOCIATIONS.PRJ_PROJECT_SKEY=
                        vgis.vgis_projects.PROJECT_SKEY)
                        AND (vgis.FREQUENCY_COUNTS.GTO_GROUP_TAXON_SKEY=
                        vgis.GROUP_TAXON_MEMBERSHIPS.GROUP_TAXON_SKEY)
                        AND (vgis.GROUP_TAXON_MEMBERSHIPS.TAXC_TAXONOMIC_CD_SKEY=
                        vgis.TAXONOMIC_NAMES.TAXONOMIC_CD_SKEY)
                        AND (vgis.POINT_LOCATION_VISITATIONS.CE_VISIT_SKEY=
                        vgis.FREQUENCY_COUNTS.CEV_CE_VISIT_SKEY)
                        AND (vgis.FREQUENCY_COUNTS.INTCL_INTERVAL_CLASS_SKEY=
                        vgis.INTERVAL_CLASSES.INTERVAL_CLASS_SKEY)
                        AND (vgis.PLOT_CLUSTERS.DES_DESIGN_SKEY=
                        vgis.DESIGN_SPECIFICATIONS.DESIGN_SKEY))

                        ORDER BY vgis.vgis_projects.PROJECT_BUSINESS_ID ASC,
                        vgis.PLOT_CLUSTERS.SAMPLE_EXP_PLOT_NUM ASC,
                        vgis.vgis_projects.PROJECT_BUSINESS_ID,
                        vgis.TAXONOMIC_NAMES.LONG_CD ASC,
                        vgis.INTERVAL_CLASSES.LO_BND ASC") %>%
    data.table
  set(c10stre, , c("CL_KEY", "PROJ_KEY", "MVST_KEY", "X",
                   "MS_DT"), NULL)
  c10stre[,':='(CLSTR_ID = getClusterID(PROJ_ID, SAMP_NO, TYPE_,
                                        INTENT, VISIT),
                PLOT = getPlotCode(PLOT_ID),
                LONG_SPECIES = toupper(LONG_SPECIES))]
  set(c10stre, , c("PROJ_ID", "SAMP_NO", "TYPE_", "INTENT", "VISIT", "PLOT_ID"), NULL)
  cardf <- c10stre[,.(CLSTR_ID, PLOT, LONG_SPECIES, LO_BND, TOTAL)]
  cardf <- cardf[!duplicated(cardf),]
  cardf[LO_BND == 0.1, low_bnd_new := 1]
  cardf[LO_BND == 0.31, low_bnd_new := 2]
  cardf[LO_BND > 1.30, low_bnd_new := 3]
  cardf[, LO_BND := NULL]
  ## found for some combinations of clster+plot+LONG_SPECIES+lowbndnew, there are some multiple
  ## total number of small trees
  ## need to talk to Bob to figure out what is going on
  ## temporary solution, summation
  cardf <- cardf[,.(TOTAL = sum(TOTAL)),
                 by = c("CLSTR_ID", "PLOT", "LONG_SPECIES", "low_bnd_new")]
  cardf <- reshape(data = cardf,
                   v.names = "TOTAL",
                   timevar = "low_bnd_new",
                   idvar = c("CLSTR_ID", "PLOT", "LONG_SPECIES"),
                   direction = "wide",
                   sep = "")
  cardef <- c10stre[,.(CLSTR_ID, PLOT, PL_TYPE, PL_QUAL, F_RAD, PL_ORIG = "SML_TR")]
  cardef <- cardef[!duplicated(cardef),]
  carde <- rbind(cardeg, cardef)
  carde <- carde[!duplicated(carde),]
  carde[PL_QUAL == "B", F_BDRY := "X"]
  carde[PL_QUAL == "S", F_SPLT := "X"]
  carde[PL_TYPE == "F", ':='(F_FULL = "X",
                             PLOT_WT = 1)]
  carde[PL_TYPE == "H", ':='(F_HALF = "X",
                             PLOT_WT = 2)]
  carde[PL_TYPE == "Q", ':='(F_QRTR = "X",
                             PLOT_WT = 4)]
  carde[, ':='(PL_TYPE = NULL,
               PL_QUAL = NULL)]
  alldata <- list(header = carde,
                  stump = cardg,
                  smalltree = cardf)
  rm(carde, cardf, cardef, cardeg)
  dbDisconnect(con)
  if(saveThem){
    saveRDS(alldata, file.path(savePath, "stumpSTree_card_vgis.rds"))
  } else if (!saveThem){
    return(alldata)
  } else{
    stop("saveThem must be logical.")
  }
}
