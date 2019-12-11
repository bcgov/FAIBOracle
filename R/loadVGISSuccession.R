#' Load the card 16 succession interpretations (eo) data from Oracle database
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
#' @rdname loadVGISSuccession
#' @author Yong Luo
loadVGISSuccession <- function(userName, passWord, saveThem = FALSE,
                               savePath = file.path(".")){
  drv <- dbDriver("Oracle")
  connect_string <- getServer(databaseName = "VGIS")
  con <- dbConnect(drv, username = userName, password = passWord,
                   dbname = connect_string)
  c16_a <- dbGetQuery(con, "SELECT
                      vgis.PLOT_CLUSTERS.PLOT_CLUSTER_SKEY                             as cl_key,
                      vgis.vgis_projects.PROJECT_SKEY                                       as proj_key,
                      vgis.MEASMT_VISITATIONS.MEASMT_VISIT_SKEY                        as vst_key,
                      vgis.vgis_projects.PROJECT_BUSINESS_ID                                as proj_id,
                      vgis.PLOT_CLUSTERS.SAMPLE_EXP_PLOT_NUM                           AS SAMP_NO,
                      vgis.DESIGN_SPECIFICATIONS.BUSINESS_IDENTIFIER_VALUE             as type_,
                      vgis.MEASMT_VISITATIONS.SAMPLE_INTENT                            as intent ,
                      vgis.MEASMT_VISITATIONS.MEASMT_VISITN_NUM                        as visit,
                      vgis.SUCCESSION_INTERPRETATIONS.SSCC_SERAL_STAGE_CD              as stage,  /* Structural Stage  */
                      vgis.SUCCESSION_INTERPRETATIONS.TREE_HARVESTING_CD               as harvest,
                      vgis.SUCCESSION_INTERPRETATIONS.SNAGS_CD                         as snags,
                      vgis.SUCCESSION_INTERPRETATIONS.CWD_IN_ALL_DECAY_STAGES_CD       as cwd, /* SNAGS/CWD  */
                      vgis.SUCCESSION_INTERPRETATIONS.CANOPY_GAPS_FROM_TREE_MORTALIT   as canopy,
                      vgis.SUCCESSION_INTERPRETATIONS.VERTICAL_STRUCTURE_CD            as struct,
                      vgis.SUCCESSION_INTERPRETATIONS.SUCCESSIONAL_STABILITY_CD        as stablty,
                      vgis.SUCCESSION_INTERPRETATIONS.TREE_AGE_FOR_SPECIES_SITE_CD     as age,
                      vgis.SUCCESSION_INTERPRETATIONS.TREE_SIZE_FOR_SPECIES_SITE_CD    as tsize,
                      vgis.SUCCESSION_INTERPRETATIONS.OLD_GROWTH_FOREST_CD             as old_grw,
                      vgis.SUCCESSION_INTERPRETATIONS.LIVE_OLD_TREES_PCT               as alv_pct
                      FROM
                      vgis.PLOT_CLUSTERS ,
                      vgis.vgis_projects,
                      vgis.PROJECT_ASSOCIATIONS,
                      vgis.MEASMT_VISITATIONS,
                      vgis.DESIGN_SPECIFICATIONS,
                      vgis.POINT_LOCATION_VISITATIONS,
                      vgis.SUCCESSION_INTERPRETATIONS
                      WHERE
                      (vgis.PROJECT_ASSOCIATIONS.PRJ_PROJECT_SKEY
                      = vgis.vgis_projects.PROJECT_SKEY)
                      AND (vgis.PROJECT_ASSOCIATIONS.PCLTR_PLOT_CLUSTER_SKEY
                      = vgis.PLOT_CLUSTERS.PLOT_CLUSTER_SKEY)
                      AND (vgis.DESIGN_SPECIFICATIONS.DESIGN_SKEY
                      = vgis.PLOT_CLUSTERS.DES_DESIGN_SKEY)
                      AND (vgis.MEASMT_VISITATIONS.PCLTR_PLOT_CLUSTER_SKEY
                      = vgis.PLOT_CLUSTERS.PLOT_CLUSTER_SKEY)
                      AND (vgis.POINT_LOCATION_VISITATIONS.MEAVT_MEASMT_VISIT_SKEY
                      = vgis.MEASMT_VISITATIONS.MEASMT_VISIT_SKEY)
                      AND (vgis.SUCCESSION_INTERPRETATIONS.CEV_CE_VISIT_SKEY
                      = vgis.POINT_LOCATION_VISITATIONS.CE_VISIT_SKEY)") %>%
    data.table

  set(c16_a, , c("CL_KEY", "PROJ_KEY", "VST_KEY"), NULL)
  c16_a[,':='(CLSTR_ID = getClusterID(PROJ_ID, SAMP_NO, TYPE_,
                                      INTENT, VISIT))]
  set(c16_a, , c("PROJ_ID", "SAMP_NO", "TYPE_", "INTENT", "VISIT"), NULL)
  c16_a1 <- c16_a[,.(CLSTR_ID, STAGE, HARVEST, SNAGS, CWD, CANOPY, STRUCT, STABLTY,
                     AGE, SIZE = TSIZE, OLD_GRW, ALV_PCT)]
  rm(c16_a)
  c16_b <- dbGetQuery(con, "SELECT
                      vgis.PLOT_CLUSTERS.PLOT_CLUSTER_SKEY         as cl_key,
                      vgis.vgis_projects.PROJECT_SKEY                   as proj_key,
                      vgis.MEASMT_VISITATIONS.MEASMT_VISIT_SKEY    as vst_key,
                      vgis.vgis_projects.PROJECT_BUSINESS_ID            as proj_id ,  /*-- Project  */
                      vgis.PLOT_CLUSTERS.SAMPLE_EXP_PLOT_NUM       AS SAMP_NO,      /*-- Plot Sample No  */
                      vgis.DESIGN_SPECIFICATIONS.BUSINESS_IDENTIFIER_VALUE as type_,      /*-- Sample Type  */
                      vgis.MEASMT_VISITATIONS.SAMPLE_INTENT        as intent,             /* -- Sample Intent  */
                      vgis.MEASMT_VISITATIONS.MEASMT_VISITN_NUM    as visit,            /* -- Sample No  */
                      vgis.VEGETATION_ESTAB_ASSMTS.RANK            as rank,
                      vgis.site_disturbance_cds.SITE_DIST_CD       as site_cd
                      FROM
                      vgis.PLOT_CLUSTERS ,
                      vgis.vgis_projects,
                      vgis.PROJECT_ASSOCIATIONS,
                      vgis.MEASMT_VISITATIONS,
                      vgis.DESIGN_SPECIFICATIONS,
                      vgis.POINT_LOCATION_VISITATIONS,
                      vgis.SUCCESSION_INTERPRETATIONS,
                      vgis.VEGETATION_ESTAB_ASSMTS,
                      vgis.site_disturbance_cds
                      WHERE
                      (vgis.PROJECT_ASSOCIATIONS.PRJ_PROJECT_SKEY
                      = vgis.vgis_projects.PROJECT_SKEY)
                      AND (vgis.PROJECT_ASSOCIATIONS.PCLTR_PLOT_CLUSTER_SKEY
                      = vgis.PLOT_CLUSTERS.PLOT_CLUSTER_SKEY)
                      AND (vgis.DESIGN_SPECIFICATIONS.DESIGN_SKEY
                      = vgis.PLOT_CLUSTERS.DES_DESIGN_SKEY)
                      AND (vgis.MEASMT_VISITATIONS.PCLTR_PLOT_CLUSTER_SKEY
                      = vgis.PLOT_CLUSTERS.PLOT_CLUSTER_SKEY)
                      AND (vgis.POINT_LOCATION_VISITATIONS.MEAVT_MEASMT_VISIT_SKEY
                      = vgis.MEASMT_VISITATIONS.MEASMT_VISIT_SKEY)
                      AND (vgis.SUCCESSION_INTERPRETATIONS.CEV_CE_VISIT_SKEY
                      = vgis.POINT_LOCATION_VISITATIONS.CE_VISIT_SKEY)
                      AND (vgis.VEGETATION_ESTAB_ASSMTS.SI_SUCCESSION_INT_SKEY
                      = vgis.SUCCESSION_INTERPRETATIONS.SUCCESSION_INT_SKEY)
                      AND (vgis.site_disturbance_cds.SITE_DISTR_CD_SKEY
                      = vgis.VEGETATION_ESTAB_ASSMTS.SDC_SITE_DISTR_CD_SKEY)") %>%
    data.table
  set(c16_b, , c("CL_KEY", "PROJ_KEY", "VST_KEY"), NULL)
  c16_b[,':='(CLSTR_ID = getClusterID(PROJ_ID, SAMP_NO, TYPE_,
                                      INTENT, VISIT))]
  set(c16_b, , c("PROJ_ID", "SAMP_NO", "TYPE_", "INTENT", "VISIT"), NULL)
  setnames(c16_b, "SITE_CD", "FACTR")
  c16_b2 <- reshape(data = c16_b,
                    v.names = "FACTR",
                    timevar = "RANK",
                    idvar = "CLSTR_ID",
                    direction = "wide",
                    sep = "_")
  rm(c16_b)

  c16_c <- dbGetQuery(con, "SELECT
                      vgis.PLOT_CLUSTERS.PLOT_CLUSTER_SKEY                 as cl_key,
                      vgis.vgis_projects.PROJECT_SKEY                           as proj_key,
                      vgis.MEASMT_VISITATIONS.MEASMT_VISIT_SKEY            as vst_key,
                      vgis.vgis_projects.PROJECT_BUSINESS_ID                    as proj_id,
                      vgis.PLOT_CLUSTERS.SAMPLE_EXP_PLOT_NUM               AS SAMP_NO,
                      vgis.DESIGN_SPECIFICATIONS.BUSINESS_IDENTIFIER_VALUE as type_,
                      vgis.MEASMT_VISITATIONS.SAMPLE_INTENT                as intent ,
                      vgis.MEASMT_VISITATIONS.MEASMT_VISITN_NUM             as visit,
                      vgis.TREE_SPECIES_SUCCESSIONS.TREE_SPP_SUCCN_CURRENCY_CD as suc_cd ,
                      vgis.TREE_SPECIES_SUCCESSIONS.SEQUENCE_NUM           as seq_no,
                      vgis.TAXONOMIC_NAMES.LONG_CD                        AS LONG_SPECIES
                      FROM
                      vgis.PLOT_CLUSTERS ,
                      vgis.vgis_projects,
                      vgis.PROJECT_ASSOCIATIONS,
                      vgis.MEASMT_VISITATIONS,
                      vgis.DESIGN_SPECIFICATIONS,
                      vgis.POINT_LOCATION_VISITATIONS,
                      vgis.SUCCESSION_INTERPRETATIONS,
                      vgis.TREE_SPECIES_SUCCESSIONS,
                      vgis.GROUP_TAXON_MEMBERSHIPS,
                      vgis.TAXONOMIC_NAMES
                      WHERE
                      (vgis.PROJECT_ASSOCIATIONS.PRJ_PROJECT_SKEY
                      = vgis.vgis_projects.PROJECT_SKEY)
                      AND (vgis.PROJECT_ASSOCIATIONS.PCLTR_PLOT_CLUSTER_SKEY
                      = vgis.PLOT_CLUSTERS.PLOT_CLUSTER_SKEY)
                      AND (vgis.DESIGN_SPECIFICATIONS.DESIGN_SKEY
                      = vgis.PLOT_CLUSTERS.DES_DESIGN_SKEY)
                      AND (vgis.MEASMT_VISITATIONS.PCLTR_PLOT_CLUSTER_SKEY
                      = vgis.PLOT_CLUSTERS.PLOT_CLUSTER_SKEY)
                      AND (vgis.POINT_LOCATION_VISITATIONS.MEAVT_MEASMT_VISIT_SKEY
                      = vgis.MEASMT_VISITATIONS.MEASMT_VISIT_SKEY)
                      AND (vgis.SUCCESSION_INTERPRETATIONS.CEV_CE_VISIT_SKEY
                      = vgis.POINT_LOCATION_VISITATIONS.CE_VISIT_SKEY)
                      AND (vgis.TREE_SPECIES_SUCCESSIONS.SI_SUCCESSION_INT_SKEY
                      = vgis.SUCCESSION_INTERPRETATIONS.SUCCESSION_INT_SKEY)
                      AND (vgis.TREE_SPECIES_SUCCESSIONS.GTO_GROUP_TAXON_SKEY
                      =vgis.GROUP_TAXON_MEMBERSHIPS.GROUP_TAXON_SKEY)
                      AND (vgis.GROUP_TAXON_MEMBERSHIPS.TAXC_TAXONOMIC_CD_SKEY
                      =vgis.TAXONOMIC_NAMES.TAXONOMIC_CD_SKEY)") %>%
    data.table
  set(c16_c, , c("CL_KEY", "PROJ_KEY", "VST_KEY"), NULL)
  c16_c[,':='(CLSTR_ID = getClusterID(PROJ_ID, SAMP_NO, TYPE_,
                                      INTENT, VISIT))]
  set(c16_c, , c("PROJ_ID", "SAMP_NO", "TYPE_", "INTENT", "VISIT"), NULL)
  c16_c <- c16_c[,.(CLSTR_ID,
                    SPEC = LONG_SPECIES,
                    newgroup = paste(SUC_CD, SEQ_NO, sep = ""))]
  c16_c <- c16_c[newgroup %in% c("PR1", "PR2", "CR1", "CR2")]
  c16_c2 <- reshape(data = c16_c,
                    v.names = "SPEC",
                    timevar = "newgroup",
                    idvar = "CLSTR_ID",
                    direction = "wide",
                    sep = "_")
  card_eo <- merge(c16_a1, c16_b2, by = "CLSTR_ID",
                   all = TRUE)
  card_eo <- merge(card_eo, c16_c2, by = "CLSTR_ID",
                   all = TRUE)
  dbDisconnect(con)
  if(saveThem){
    saveRDS(card_eo, file.path(savePath, "eo_card_vgis.rds"))
  } else if (!saveThem){
    return(card_eo)
  } else{
    stop("saveThem must be logical.")
  }
}
