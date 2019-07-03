#' Load the count tree data from Oracle database
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
#' @rdname loadVGISTreeI
#' @author Yong Luo
loadVGISTreeI <- function(userName, password, saveThem = FALSE,
                          savePath = file.path(".")){
  drv <- dbDriver("Oracle")
  connect.string <-"(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)
  (HOST=nrk1-scan.bcgov)(PORT=1521))
  (CONNECT_DATA=(SERVER=DEDICATED)(SERVICE_NAME=ENVPROD1.NRS.BCGOV)))"
  con <- dbConnect(drv, username = userName, password = password,
                   dbname = connect.string)
  c11tree <- dbGetQuery(con, "SELECT
                        vgis.PLOT_CLUSTERS.PLOT_CLUSTER_SKEY                 as CL_KEY,
                        vgis.vgis_projects.PROJECT_SKEY                           as PROJ_KEY,
                        vgis.vgis_projects.PROJECT_BUSINESS_ID                    as PROJ_ID,
                        vgis.PLOT_CLUSTERS.SAMPLE_EXP_PLOT_NUM               as SAMP_NO,
                        vgis.DESIGN_SPECIFICATIONS.BUSINESS_IDENTIFIER_VALUE as TYPE_,
                        vgis.MEASMT_VISITATIONS.SAMPLE_INTENT                as INTENT,
                        vgis.MEASMT_VISITATIONS.MEASMT_VISITN_NUM            as VISIT,
                        vgis.CLUSTER_ELEMENTS.PLOT_IDENTIFIER                as PLOT_ID,
                        vgis.SPATIAL_DELIMITERS.SPTLD_TYPE                   as X,
                        vgis.SPATIAL_DELIMITERS.BASAL_AREA_FTR               as V_BAF,
                        vgis.SPATIAL_DELIMITERS.RADIUS                       as F_RAD,
                        vgis.POINT_LOCATION_VISITATIONS.PLOT_TYPE_CD         as PL_TYPE,
                        vgis.POINT_LOCATION_VISITATIONS.PLOT_QUALIFIER_CD    as PL_QUAL,
                        vgis.TREES.TREE_NUM                                  as tree_num,
                        vgis.TAXONOMIC_NAMES.LONG_CD                        AS LONG_SPECIES,
                        vgis.TREE_MEASMTS.TREE_EXTANT_CD                     as L_D,
                        vgis.TREE_MEASMTS.TREE_STANDING_IND                  AS S_F,
                        vgis.DIAM_ASSMTS.MEASMT_HT                           as DIAM_HT,
                        vgis.DIAM_ASSMTS.DIAMETER                            as DIAM,
                        vgis.DIAM_ASSMTS.ASSMT_QUAL_CD                       as DIAM_QL,
                        vgis.DIAM_ASSMTS.POSITION_CD                         as DIAM_POS

                        FROM
                        vgis.vgis_projects,
                        vgis.PROJECT_ASSOCIATIONS,
                        vgis.POINT_LOCATION_VISITATIONS,
                        vgis.POINT_LOCATIONS,
                        vgis.PLOT_CLUSTERS,
                        vgis.MEASMT_VISITATIONS,
                        vgis.DIAM_ASSMTS,
                        vgis.CLUSTER_ELEMENTS,
                        vgis.TREE_MEASMTS,
                        vgis.BIOMETRIC_DESIGN_SPECIFICATION,
                        vgis.SPATIAL_DELIMITERS,
                        vgis.TREES,
                        vgis.TAXONOMIC_NAMES,
                        vgis.GROUP_TAXON_MEMBERSHIPS,
                        vgis.DESIGN_SPECIFICATIONS
                        WHERE
                        (vgis.MEASMT_VISITATIONS.PSTAT_PROCESS_STATE_CD='DDL'
                        AND vgis.TREES.TREE_TYPE='IND' AND
                        vgis.POINT_LOCATIONS.POINT_TYPE = 'AUX PLT CR') AND
                        ((vgis.PROJECT_ASSOCIATIONS.PCLTR_PLOT_CLUSTER_SKEY
                        =vgis.PLOT_CLUSTERS.PLOT_CLUSTER_SKEY) AND
                        (vgis.PROJECT_ASSOCIATIONS.PRJ_PROJECT_SKEY
                        =vgis.vgis_projects.PROJECT_SKEY) AND
                        (vgis.CLUSTER_ELEMENTS.BDSPEC_BD_SPEC_SKEY
                        =vgis.BIOMETRIC_DESIGN_SPECIFICATION.BD_SPEC_SKEY) AND
                        (vgis.CLUSTER_ELEMENTS.CLUSTER_ELEMENT_SKEY
                        =vgis.POINT_LOCATION_VISITATIONS.CELMT_CLUSTER_ELEMENT_SKEY) AND
                        (vgis.SPATIAL_DELIMITERS.SPATIAL_DELIM_SKEY
                        =vgis.BIOMETRIC_DESIGN_SPECIFICATION.SPTLD_SPATIAL_DELIM_SKEY) AND
                        (vgis.MEASMT_VISITATIONS.PCLTR_PLOT_CLUSTER_SKEY
                        =vgis.PLOT_CLUSTERS.PLOT_CLUSTER_SKEY) AND
                        (vgis.MEASMT_VISITATIONS.MEASMT_VISIT_SKEY
                        =vgis.POINT_LOCATION_VISITATIONS.MEAVT_MEASMT_VISIT_SKEY) AND
                        (vgis.POINT_LOCATIONS.POINT_LOCN_SKEY
                        =vgis.POINT_LOCATION_VISITATIONS.PLOC_POINT_LOCN_SKEY) AND
                        (vgis.TREE_MEASMTS.CEV_CE_VISIT_SKEY
                        =vgis.POINT_LOCATION_VISITATIONS.CE_VISIT_SKEY) AND
                        (vgis.TREES.TREE_SKEY
                        =vgis.TREE_MEASMTS.TREE_TREE_SKEY) AND
                        (vgis.DIAM_ASSMTS.TRMEA_TREE_MEASMT_SKEY
                        =vgis.TREE_MEASMTS.TREE_MEASMT_SKEY) AND
                        (vgis.TREES.GTO_GROUP_TAXON_SKEY
                        =vgis.GROUP_TAXON_MEMBERSHIPS.GROUP_TAXON_SKEY) AND
                        (vgis.GROUP_TAXON_MEMBERSHIPS.TAXC_TAXONOMIC_CD_SKEY
                        =vgis.TAXONOMIC_NAMES.TAXONOMIC_CD_SKEY) AND
                        (vgis.DESIGN_SPECIFICATIONS.DESIGN_SKEY
                        =vgis.PLOT_CLUSTERS.DES_DESIGN_SKEY ))") %>%
    data.table
  set(c11tree, , c("CL_KEY", "PROJ_KEY", "X", "DIAM_QL",
                 "V_BAF",  "F_RAD", "PL_QUAL", "PL_TYPE"), NULL)
  c11tree <- c11tree[!(PLOT_ID %in% c(1, 6)), ]
  c11tree <- c11tree[DIAM_POS != "BRK",]
  c11tree[,':='(CLSTR_ID = getClusterID(PROJ_ID, SAMP_NO, TYPE_,
                                        INTENT, VISIT),
                PLOT = getPlotCode(PLOT_ID),
                TREE_NO = getTreeID(TREE_NUM),
                LV_D = substr(L_D, 1, 1),
                LONG_SPECIES = toupper(LONG_SPECIES))]
  c11tree <- c11tree[PROJ_ID != "LGMW"]
  set(c11tree, , c("PROJ_ID", "SAMP_NO", "TYPE_", "INTENT", "VISIT",
                   "PLOT_ID", "TREE_NUM", "L_D"), NULL)
  c11tree <- c11tree[!duplicated(c11tree),]
  c11tree[DIAM_HT == 1.3 & DIAM_POS == "BOL", DBH := DIAM]
  c11tree[, ':='(DIAM_HT = NULL,
                 DIAM = NULL,
                 DIAM_POS = NULL)]
  c11tree <- c11tree[,.(CLSTR_ID, PLOT, TREE_NO, LONG_SPECIES, DBH, LV_D, S_F)]
  if(nrow(c11tree) != nrow(unique(c11tree, by = c("CLSTR_ID", "PLOT", "TREE_NO")))){
    warning("Some count trees have multiple records of DBH or LONG_SPECIES.")
  }
    dbDisconnect(con)
  if(saveThem){
    saveRDS(c11tree, file.path(savePath, "tree_cardi_vgis.rds"))
  } else if (!saveThem){
    return(c11tree)
  } else{
    stop("saveThem must be logical.")
  }
}
