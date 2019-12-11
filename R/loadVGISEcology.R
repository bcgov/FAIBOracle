#' Load the ecological data from Oracle database
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
#' @rdname loadVGISEcology
#' @author Yong Luo
loadVGISEcology <- function(userName, passWord, saveThem = FALSE,
                            savePath = file.path(".")){
  drv <- dbDriver("Oracle")
  connect_string <- getServer(databaseName = "VGIS")
  con <- dbConnect(drv, username = userName, password = passWord,
                   dbname = connect_string)
  c12becz <- dbGetQuery(con, "SELECT
                        vgis.PLOT_CLUSTERS.PLOT_CLUSTER_SKEY AS CL_KEY,
                        vgis.vgis_projects.PROJECT_SKEY AS PROJ_KEY,
                        vgis.vgis_projects.PROJECT_BUSINESS_ID AS PROJ_ID,
                        vgis.PLOT_CLUSTERS.SAMPLE_EXP_PLOT_NUM AS SAMP_NO,
                        vgis.DESIGN_SPECIFICATIONS.BUSINESS_IDENTIFIER_VALUE AS TYPE_,
                        vgis.MEASMT_VISITATIONS.SAMPLE_INTENT AS INTENT,
                        vgis.MEASMT_VISITATIONS.MEASMT_VISITN_NUM AS VISIT,
                        vgis.SITE_FEATURE_ASSMTS.SITE_FEATURE_SKEY AS FEAT_KEY,
                        vgis.BGC_ZONAL_CLASSFN_CDS.BGC_CD AS BECZONE,
                        vgis.BGC_ZONAL_CLASSFN_LEVELS.LEVEL_NUM AS nLEVEL,
                        vgis.BGC_ZONAL_CLASSFN_LEVELS.LEVEL_CD AS BECSBZN

                        FROM    vgis.MEASMT_VISITATIONS,
                        vgis.PLOT_CLUSTERS,
                        vgis.POINT_LOCATION_VISITATIONS,
                        vgis.vgis_projects,
                        vgis.PROJECT_ASSOCIATIONS,
                        vgis.SITE_FEATURE_ASSMTS,
                        vgis.BGC_ZONAL_CLASSFN_CDS,
                        vgis.BGC_ZONAL_CLASSFN_LEVELS,
                        vgis.DESIGN_SPECIFICATIONS

                        WHERE   vgis.MEASMT_VISITATIONS.PSTAT_PROCESS_STATE_CD='DDL'
                        AND  (vgis.MEASMT_VISITATIONS.PCLTR_PLOT_CLUSTER_SKEY
                        =vgis.PLOT_CLUSTERS.PLOT_CLUSTER_SKEY)
                        AND (vgis.POINT_LOCATION_VISITATIONS.MEAVT_MEASMT_VISIT_SKEY
                        =vgis.MEASMT_VISITATIONS.MEASMT_VISIT_SKEY)
                        AND (vgis.PROJECT_ASSOCIATIONS.PCLTR_PLOT_CLUSTER_SKEY
                        = vgis.PLOT_CLUSTERS.PLOT_CLUSTER_SKEY)
                        AND (vgis.PROJECT_ASSOCIATIONS.PRJ_PROJECT_SKEY
                        = vgis.vgis_projects.PROJECT_SKEY)
                        AND (vgis.PLOT_CLUSTERS.DES_DESIGN_SKEY
                        =vgis.DESIGN_SPECIFICATIONS.DESIGN_SKEY)
                        and (vgis.site_feature_assmts.cev_ce_visit_skey
                        = vgis.point_location_visitations.ce_visit_skey)
                        AND (vgis.BGC_ZONAL_CLASSFN_CDS.BGCL_LEVEL_NUM
                        = vgis.BGC_ZONAL_CLASSFN_LEVELS.LEVEL_NUM )
                        AND vgis.BGC_ZONAL_CLASSFN_CDS.BGC_SKEY IN (
                        SELECT BGC_SKEY
                        FROM BGC_ZONAL_CLASSFN_CDS ZC2
                        CONNECT BY PRIOR ZC2.BGZC_BGC_SKEY = ZC2.BGC_SKEY
                        START WITH ZC2.BGC_SKEY = vgis.SITE_FEATURE_ASSMTS.BGZC_BGC_SKEY )") %>%
    data.table
  set(c12becz, , c("CL_KEY", "PROJ_KEY", "FEAT_KEY"), NULL)
  c12becz[,':='(CLSTR_ID = getClusterID(PROJ_ID, SAMP_NO, TYPE_,
                                      INTENT, VISIT))]
  set(c12becz, , c("PROJ_ID", "SAMP_NO", "TYPE_", "INTENT", "VISIT"), NULL)
  c12becz <- c12becz[!duplicated(c12becz),.(CLSTR_ID, BECZONE, NLEVEL)]
  exitinglevels <- unique(c12becz$NLEVEL)
  alllevels <- 1:4
  missingcol <- alllevels[!(alllevels %in% exitinglevels)]
  c12becz <- reshape(c12becz, v.names = "BECZONE",
                     timevar = "NLEVEL",
                     idvar = "CLSTR_ID",
                     direction = "wide")
  c12becz[, paste("BECZONE.", missingcol, sep = "") := NA]
  setnames(c12becz, paste("BECZONE.", 1:4, sep = ""),
             c("BZONE", "BSBZONE", "BEC_VAR", "BEC_P"))


  c12horz <- dbGetQuery(con, "SELECT
                        vgis.PLOT_CLUSTERS.PLOT_CLUSTER_SKEY AS CL_KEY,
                        vgis.vgis_projects.PROJECT_SKEY AS PROJ_KEY,
                        vgis.vgis_projects.PROJECT_BUSINESS_ID AS PROJ_ID,
                        vgis.PLOT_CLUSTERS.SAMPLE_EXP_PLOT_NUM AS SAMP_NO,
                        vgis.DESIGN_SPECIFICATIONS.BUSINESS_IDENTIFIER_VALUE AS TYPE_,
                        vgis.MEASMT_VISITATIONS.SAMPLE_INTENT AS INTENT,
                        vgis.MEASMT_VISITATIONS.MEASMT_VISITN_NUM AS VISIT,
                        vgis.ECOSYSTEM_DESC_DETAILS.PLOT_CENTRE_IND AS PLOT_CNT,
                        vgis.ECOSYSTEM_DESC_DETAILS.ECOSYS_DESCR_SKEY AS DESC_KEY,
                        vgis.SITE_FEATURE_ASSMTS.SITE_FEATURE_SKEY AS FEAT_KEY,
                        vgis.SITE_FEATURE_ASSMTS.SOIL_FEATURE_APPLICABILITY_IND AS Depth_N_Appl,
                        vgis.SOIL_HORIZONS.HORIZON_CD AS HORIZON,
                        vgis.SOIL_HORIZONS.DEPTH AS DEPTH,
                        vgis.SOIL_HORIZONS.TEXTURE_CD AS TEXTURE,
                        vgis.SOIL_HORIZONS.COARSE_FRAGMENTS_TOTAL_PCT AS TOTAL,
                        vgis.SOIL_HORIZONS.GRAVEL_PCT AS GRAVEL,
                        vgis.SOIL_HORIZONS.COBBLES_STONES_PCT AS COBBLES

                        FROM vgis.MEASMT_VISITATIONS,
                        vgis.PLOT_CLUSTERS,
                        vgis.POINT_LOCATION_VISITATIONS,
                        vgis.vgis_projects,
                        vgis.PROJECT_ASSOCIATIONS,
                        vgis.ECOSYSTEM_DESC_DETAILS,
                        vgis.SITE_FEATURE_ASSMTS,
                        vgis.SOIL_HORIZONS,
                        vgis.DESIGN_SPECIFICATIONS

                        WHERE vgis.MEASMT_VISITATIONS.PSTAT_PROCESS_STATE_CD='DDL'
                        AND  ((vgis.MEASMT_VISITATIONS.PCLTR_PLOT_CLUSTER_SKEY
                        =vgis.PLOT_CLUSTERS.PLOT_CLUSTER_SKEY)
                        AND (vgis.POINT_LOCATION_VISITATIONS.MEAVT_MEASMT_VISIT_SKEY
                        =vgis.MEASMT_VISITATIONS.MEASMT_VISIT_SKEY)
                        AND (vgis.PROJECT_ASSOCIATIONS.PCLTR_PLOT_CLUSTER_SKEY
                        = vgis.PLOT_CLUSTERS.PLOT_CLUSTER_SKEY)
                        AND (vgis.PROJECT_ASSOCIATIONS.PRJ_PROJECT_SKEY
                        = vgis.vgis_projects.PROJECT_SKEY)
                        AND (vgis.ECOSYSTEM_DESC_DETAILS.CEV_CE_VISIT_SKEY
                        =vgis.POINT_LOCATION_VISITATIONS.CE_VISIT_SKEY)
                        AND (vgis.SITE_FEATURE_ASSMTS.ESDD_ECOSYS_DESCR_SKEY
                        =vgis.ECOSYSTEM_DESC_DETAILS.ECOSYS_DESCR_SKEY)
                        AND (vgis.SOIL_HORIZONS.SFEA_SITE_FEATURE_SKEY
                        =vgis.SITE_FEATURE_ASSMTS.SITE_FEATURE_SKEY)
                        AND (vgis.PLOT_CLUSTERS.DES_DESIGN_SKEY
                        =vgis.DESIGN_SPECIFICATIONS.DESIGN_SKEY))") %>%
    data.table
  set(c12horz, , c("CL_KEY", "PROJ_KEY", "DESC_KEY", "FEAT_KEY"), NULL)
  c12horz[,':='(CLSTR_ID = getClusterID(PROJ_ID, SAMP_NO, TYPE_,
                                      INTENT, VISIT))]
  set(c12horz, , c("PROJ_ID", "SAMP_NO", "TYPE_", "INTENT", "VISIT"), NULL)


  c12soil <- dbGetQuery(con, "SELECT
                        vgis.PLOT_CLUSTERS.PLOT_CLUSTER_SKEY AS CL_KEY,
                        vgis.vgis_projects.PROJECT_SKEY AS PROJ_KEY,
                        vgis.vgis_projects.PROJECT_BUSINESS_ID AS PROJ_ID,
                        vgis.PLOT_CLUSTERS.SAMPLE_EXP_PLOT_NUM AS SAMP_NO,
                        vgis.DESIGN_SPECIFICATIONS.BUSINESS_IDENTIFIER_VALUE AS TYPE_,
                        vgis.MEASMT_VISITATIONS.SAMPLE_INTENT AS INTENT,
                        vgis.MEASMT_VISITATIONS.MEASMT_VISITN_NUM AS VISIT,
                        vgis.ECOSYSTEM_DESC_DETAILS.PLOT_CENTRE_IND AS PLOT_CNT ,
                        vgis.ECOSYSTEM_DESC_DETAILS.ECOSYS_DESCR_SKEY AS DESC_KEY,
                        vgis.SITE_FEATURE_ASSMTS.SITE_FEATURE_SKEY AS FEAT_KEY,
                        vgis.SITE_FEATURE_ASSMTS.SOIL_FEATURE_APPLICABILITY_IND AS Depth_NA,
                        vgis.SOIL_FEATURE_DEPTHS.DEPTH AS DEPTH,
                        vgis.SOIL_FEATURE_DEPTHS.SOIL_FEATURE_NAME AS FEATUR

                        FROM vgis.MEASMT_VISITATIONS,
                        vgis.PLOT_CLUSTERS,
                        vgis.POINT_LOCATION_VISITATIONS,
                        vgis.vgis_projects,
                        vgis.PROJECT_ASSOCIATIONS,
                        vgis.ECOSYSTEM_DESC_DETAILS,
                        vgis.SITE_FEATURE_ASSMTS,
                        vgis.SOIL_FEATURE_DEPTHS,
                        vgis.DESIGN_SPECIFICATIONS

                        WHERE vgis.MEASMT_VISITATIONS.PSTAT_PRoCESS_STATE_CD='DDL'
                        AND  ((vgis.MEASMT_VISITATIONS.PCLTR_PLOT_CLUSTER_SKEY
                        =vgis.PLOT_CLUSTERS.PLOT_CLUSTER_SKEY)
                        AND (vgis.POINT_LOCATION_VISITATIONS.MEAVT_MEASMT_VISIT_SKEY
                        =vgis.MEASMT_VISITATIONS.MEASMT_VISIT_SKEY)
                        AND (vgis.PROJECT_ASSOCIATIONS.PCLTR_PLOT_CLUSTER_SKEY
                        = vgis.PLOT_CLUSTERS.PLOT_CLUSTER_SKEY)
                        AND (vgis.PROJECT_ASSOCIATIONS.PRJ_PROJECT_SKEY
                        = vgis.vgis_projects.PROJECT_SKEY)
                        AND (vgis.PLOT_CLUSTERS.DES_DESIGN_SKEY
                        =vgis.DESIGN_SPECIFICATIONS.DESIGN_SKEY)
                        AND (vgis.ECOSYSTEM_DESC_DETAILS.CEV_CE_VISIT_SKEY
                        =vgis.POINT_LOCATION_VISITATIONS.CE_VISIT_SKEY)
                        AND (vgis.SITE_FEATURE_ASSMTS.ESDD_ECOSYS_DESCR_SKEY
                        =vgis.ECOSYSTEM_DESC_DETAILS.ECOSYS_DESCR_SKEY)
                        AND (vgis.SOIL_FEATURE_DEPTHS.SFEA_SITE_FEATURE_SKEY (+)
                        =vgis.SITE_FEATURE_ASSMTS.SITE_FEATURE_SKEY ))") %>%
    data.table
  set(c12soil, , c("CL_KEY", "PROJ_KEY", "DESC_KEY", "FEAT_KEY"), NULL)
  c12soil[,':='(CLSTR_ID = getClusterID(PROJ_ID, SAMP_NO, TYPE_,
                                      INTENT, VISIT))]
  set(c12soil, , c("PROJ_ID", "SAMP_NO", "TYPE_", "INTENT", "VISIT"), NULL)
  c12soil <- c12soil[!duplicated(c12soil),
                     .(CLSTR_ID, PLOT_CNT, DEPTH, FEATUR)]
  c12soil_nafeat <- c12soil[is.na(FEATUR),]
  c12soil <- c12soil[!is.na(FEATUR),]
  c12soil <- reshape(data = c12soil,
                          v.names = "DEPTH",
                          timevar = "FEATUR",
                          idvar = c("CLSTR_ID", "PLOT_CNT"),
                          direction = "wide")
  alloldnames <- paste("DEPTH.", c("WATER TBL", "MOTTLING", "RT RES PAN",
                                   "BEDROCK", "FROZEN LYR", "CARBONATE", "N",
                                   "GLEYING", "PIT DEPTH"), sep = "")
  allnewnames <- c("W_TABLE", "MOTTLE", "ROOT_RES",
                   "BEDROCK2", "FROZEN", "CARBON", "N",
                   "GLEYING", "PIT DEPTH")
  c12soil[, alloldnames[!(alloldnames %in% names(c12soil))] := NA]
  setnames(c12soil,alloldnames,
           allnewnames)
  c12soil <- rbindlist(list(c12soil, c12soil_nafeat[,.(CLSTR_ID, PLOT_CNT)]),
                       fill = TRUE)
  rm(c12soil_nafeat)


c12eco <- dbGetQuery(con, "SELECT
                       vgis.PLOT_CLUSTERS.PLOT_CLUSTER_SKEY AS CL_KEY,
                       vgis.vgis_projects.PROJECT_SKEY AS PROJ_KEY,
                       vgis.vgis_projects.PROJECT_BUSINESS_ID AS PROJ_ID,
                       vgis.PLOT_CLUSTERS.SAMPLE_EXP_PLOT_NUM AS SAMP_NO,
                       vgis.DESIGN_SPECIFICATIONS.BUSINESS_IDENTIFIER_VALUE AS TYPE_,
                       vgis.MEASMT_VISITATIONS.SAMPLE_INTENT AS INTENT,
                       vgis.MEASMT_VISITATIONS.MEASMT_VISITN_NUM AS VISIT,
                       vgis.ECOSYSTEM_DESC_DETAILS.ECOSYS_DESCR_SKEY AS DESC_KEY,
                       vgis.ECOSYSTEM_DESC_DETAILS.PLOT_CENTRE_IND AS PLOT_CNT,
                       vgis.ECOSYSTEM_DESC_DETAILS.FIELD_SEQ_NUM AS SEQ_NO,
                       vgis.ECOSYSTEM_DESC_DETAILS.SOIL_MOISTURE_UPPER_BND AS MOIST,
                       vgis.ECOSYSTEM_DESC_DETAILS.SOIL_NUTRIENT_UPPER_BND AS NUTR,
                       vgis.ECOSYSTEM_DESC_DETAILS.SITE_SERIES_COVERAGE AS SERPER,
                       vgis.SITE_FEATURE_ASSMTS.SITE_FEATURE_SKEY AS FEAT_KEY,
                       vgis.SITE_FEATURE_ASSMTS.ECOSYSTEM_UNIFORMITY_CD AS UFORMITY,
                       vgis.SITE_FEATURE_ASSMTS.SLOPE_PCT AS SLOPE,
                       vgis.SITE_FEATURE_ASSMTS.ASPECT_BRG AS ASPECT,
                       vgis.SITE_FEATURE_ASSMTS.ELEVATION AS ELEV,
                       vgis.SITE_FEATURE_ASSMTS.SURFACE_SHAPE_CD AS SURFACE,
                       vgis.SITE_FEATURE_ASSMTS.MESO_SLOPE_POS_CD AS MESO,
                       vgis.SITE_FEATURE_ASSMTS.MICROTOPOGRAPHY_CD AS TOPO,
                       vgis.SITE_FEATURE_ASSMTS.ROCK_FRAGMENT_SUBSTRATE_PCT AS STONES,
                       vgis.SITE_FEATURE_ASSMTS.BEDROCK_SUBSTRATE_PCT AS BEDROCK,
                       vgis.SITE_FEATURE_ASSMTS.SLOPE_FAILURE_INTEGERATED_IND AS SF_PLOT,
                       vgis.SITE_FEATURE_ASSMTS.SLOPE_FAILURE_BETWEEN_IND AS SF_BET,
                       vgis.SITE_FEATURE_ASSMTS.MAIN_PLOT_GULLIES_IND AS GUL_PLOT,
                       vgis.SITE_FEATURE_ASSMTS.BETWEEN_PLOT_GULLIES_IND AS GUL_BET,
                       vgis.SITE_FEATURE_ASSMTS.FLOOD_HAZARD_CD AS FLOOD,
                       vgis.SITE_FEATURE_ASSMTS.FLOWING_WATER_PCT AS W_FLOW,
                       vgis.SITE_FEATURE_ASSMTS.STANDING_WATER_PCT AS W_STAND,
                       vgis.SITE_FEATURE_ASSMTS.SURFICIAL_MATERIAL_1_CD AS SURF_1,
                       vgis.SITE_FEATURE_ASSMTS.SURFICIAL_MATERIAL_2_CD AS SURF_2,
                       vgis.SITE_FEATURE_ASSMTS.HUMUS_FORM_CD AS HUMUS,
                       vgis.SITE_FEATURE_ASSMTS.ROOT_ZONE_SOIL_COLOR_CD AS SOIL_COL,
                       vgis.SITE_FEATURE_ASSMTS.SOIL_FEATURE_APPLICABILITY_IND AS SOIL_NA,
                       vgis.SITE_SERIES_CDS.SITE_SERIES_CD AS SERNO

                       FROM vgis.MEASMT_VISITATIONS,
                       vgis.PLOT_CLUSTERS,
                       vgis.POINT_LOCATION_VISITATIONS,
                       vgis.vgis_projects,
                       vgis.PROJECT_ASSOCIATIONS,
                       vgis.ECOSYSTEM_DESC_DETAILS,
                       vgis.SITE_FEATURE_ASSMTS,
                       vgis.SITE_SERIES_CDS,
                       vgis.DESIGN_SPECIFICATIONS

                       WHERE vgis.MEASMT_VISITATIONS.PSTAT_PROCESS_STATE_CD='DDL'
                       AND  ((vgis.MEASMT_VISITATIONS.PCLTR_PLOT_CLUSTER_SKEY
                       =vgis.PLOT_CLUSTERS.PLOT_CLUSTER_SKEY)
                       AND (vgis.POINT_LOCATION_VISITATIONS.MEAVT_MEASMT_VISIT_SKEY
                       =vgis.MEASMT_VISITATIONS.MEASMT_VISIT_SKEY)
                       AND (vgis.PROJECT_ASSOCIATIONS.PCLTR_PLOT_CLUSTER_SKEY
                       = vgis.PLOT_CLUSTERS.PLOT_CLUSTER_SKEY)
                       AND (vgis.PROJECT_ASSOCIATIONS.PRJ_PROJECT_SKEY
                       = vgis.vgis_projects.PROJECT_SKEY)
                       AND (vgis.PLOT_CLUSTERS.DES_DESIGN_SKEY
                       =vgis.DESIGN_SPECIFICATIONS.DESIGN_SKEY)
                       AND (vgis.ECOSYSTEM_DESC_DETAILS.CEV_CE_VISIT_SKEY
                       =vgis.POINT_LOCATION_VISITATIONS.CE_VISIT_SKEY)
                       AND (vgis.SITE_FEATURE_ASSMTS.ESDD_ECOSYS_DESCR_SKEY(+)
                       =vgis.ECOSYSTEM_DESC_DETAILS.ECOSYS_DESCR_SKEY)
                       AND (vgis.ECOSYSTEM_DESC_DETAILS.SS_SITE_SERIES_SKEY
                       =vgis.SITE_SERIES_CDS.SITE_SERIES_SKEY(+)))") %>%
    data.table
  set(c12eco, , c("CL_KEY", "PROJ_KEY", "DESC_KEY", "FEAT_KEY"), NULL)
  c12eco[,':='(CLSTR_ID = getClusterID(PROJ_ID, SAMP_NO, TYPE_,
                                      INTENT, VISIT))]
  set(c12eco, , c("PROJ_ID", "SAMP_NO", "TYPE_", "INTENT", "VISIT"), NULL)
  ecob <- c12eco[SEQ_NO == 1 |
                   (SEQ_NO == 2 & (!is.na(ELEV) |
                                     !is.na(ASPECT) |
                                     !is.na(SLOPE))),]
  set(ecob, , c("MOIST", "NUTR", "SEQ_NO", "SERNO", "SERPER"), NULL)

  c12eco1 <- c12eco[,.(CLSTR_ID, MOIST, NUTR, SERNO, SERPER, SEQ_NO)]
  c12eco1 <- c12eco1[!duplicated(c12eco1),]
  maxseq <- max(c12eco1$SEQ_NO)
  ecoa <- reshape(data = c12eco1,
                          v.names = c("MOIST", "NUTR", "SERNO", "SERPER"),
                          timevar = "SEQ_NO",
                          idvar = "CLSTR_ID",
                          direction = "wide",
                          sep = "_")
  rm(c12eco1)
  c12eco <- merge(ecoa, ecob,
                  by = c("CLSTR_ID"), all = TRUE)
  allclplot <- paste(c12soil$CLSTR_ID, "_", c12soil$PLOT_CNT, sep = "")
  c12eco <- c12eco[paste(CLSTR_ID, "_", PLOT_CNT, sep = "") %in% allclplot,]
  rm(ecoa, ecob, allclplot)


  c12bclnd <- dbGetQuery(con, "SELECT
                         vgis.PLOT_CLUSTERS.PLOT_CLUSTER_SKEY AS CL_KEY,
                         vgis.vgis_projects.PROJECT_SKEY AS PROJ_KEY,
                         vgis.vgis_projects.PROJECT_BUSINESS_ID AS PROJ_ID,
                         vgis.PLOT_CLUSTERS.SAMPLE_EXP_PLOT_NUM AS SAMP_NO,
                         vgis.DESIGN_SPECIFICATIONS.BUSINESS_IDENTIFIER_VALUE AS TYPE_,
                         vgis.MEASMT_VISITATIONS.SAMPLE_INTENT AS INTENT,
                         vgis.MEASMT_VISITATIONS.MEASMT_VISITN_NUM AS VISIT,
                         vgis.ECOSYSTEM_DESC_DETAILS.ECOSYS_DESCR_SKEY AS DESC_KEY,
                         vgis.ECOSYSTEM_DESC_DETAILS.FIELD_SEQ_NUM AS SEQ_NO,
                         vgis.BC_LAND_COVER_CLASS_CDS.CLASSIFICATION_CD AS CLASS,
                         vgis.BC_LAND_COVER_LEVEL_CDS.CLASSIFICATION_LEVEL AS cLEVEL

                         FROM    vgis.MEASMT_VISITATIONS,
                         vgis.PLOT_CLUSTERS,
                         vgis.POINT_LOCATION_VISITATIONS,
                         vgis.vgis_projects,
                         vgis.PROJECT_ASSOCIATIONS,
                         vgis.ECOSYSTEM_DESC_DETAILS,
                         vgis.BC_LAND_COVER_CLASS_CDS,
                         vgis.BC_LAND_COVER_LEVEL_CDS,
                         vgis.DESIGN_SPECIFICATIONS

                         WHERE   vgis.MEASMT_VISITATIONS.PSTAT_PROCESS_STATE_CD='DDL'
                         AND  (vgis.MEASMT_VISITATIONS.PCLTR_PLOT_CLUSTER_SKEY
                         =vgis.PLOT_CLUSTERS.PLOT_CLUSTER_SKEY)
                         AND (vgis.POINT_LOCATION_VISITATIONS.MEAVT_MEASMT_VISIT_SKEY
                         =vgis.MEASMT_VISITATIONS.MEASMT_VISIT_SKEY)
                         AND (vgis.PROJECT_ASSOCIATIONS.PCLTR_PLOT_CLUSTER_SKEY
                         = vgis.PLOT_CLUSTERS.PLOT_CLUSTER_SKEY)
                         AND (vgis.PROJECT_ASSOCIATIONS.PRJ_PROJECT_SKEY
                         = vgis.vgis_projects.PROJECT_SKEY)
                         AND (vgis.ECOSYSTEM_DESC_DETAILS.CEV_CE_VISIT_SKEY
                         =vgis.POINT_LOCATION_VISITATIONS.CE_VISIT_SKEY)
                         AND (vgis.BC_LAND_COVER_CLASS_CDS.LCLC_CLASSIFICATION_LEVEL
                         =vgis.BC_LAND_COVER_LEVEL_CDS.CLASSIFICATION_LEVEL )
                         AND (vgis.PLOT_CLUSTERS.DES_DESIGN_SKEY
                         =vgis.DESIGN_SPECIFICATIONS.DESIGN_SKEY)
                         AND vgis.BC_LAND_COVER_CLASS_CDS.CLASS_CODE_SKEY IN (
                         SELECT CLASS_CODE_SKEY
                         FROM BC_LAND_COVER_CLASS_CDS BC2
                         CONNECT BY PRIOR BC2.LCC_CLASS_CODE_SKEY = BC2.CLASS_CODE_SKEY
                         START WITH BC2.CLASS_CODE_SKEY  = vgis.ECOSYSTEM_DESC_DETAILS.LCC_CLASS_CODE_SKEY)
                         ORDER BY PROJ_ID, SAMP_NO, SEQ_NO, CLEVEL") %>%
    data.table
  set(c12bclnd, , c("CL_KEY", "PROJ_KEY", "DESC_KEY"), NULL)
  c12bclnd[,':='(CLSTR_ID = getClusterID(PROJ_ID, SAMP_NO, TYPE_,
                                      INTENT, VISIT))]
  set(c12bclnd, , c("PROJ_ID", "SAMP_NO", "TYPE_", "INTENT", "VISIT"), NULL)
  c12bclnd <- c12bclnd[!duplicated(c12bclnd),]
  allclsters <- unique(c12bclnd[,.(CLSTR_ID)], by = "CLSTR_ID")
  for(i in 1:5){
    subdata <- c12bclnd[CLEVEL == i,.(CLSTR_ID, SEQ_NO, CLASS)]
    maxseqno <- max(subdata$SEQ_NO)
    subdata <- reshape(data = subdata,
                       v.names = "CLASS",
                       timevar = "SEQ_NO",
                       idvar = "CLSTR_ID",
                       direction = "wide")
    setnames(subdata, paste("CLASS.", 1:maxseqno, sep = ""),
             paste("L", i, "_", 1:maxseqno, sep = ""))
    allclsters <- merge(allclsters, subdata,
                        by = "CLSTR_ID",
                        all.x = TRUE)
    rm(subdata)
  }
  c12bclnd <- allclsters
  rm(allclsters)

  alldata <- list(ecol = c12eco,
                  soil = c12soil,
                  becz = c12becz,
                  horz = c12horz,
                  bcland = c12bclnd)
  rm(c12eco, c12soil, c12becz,
       c12horz, c12bclnd)
  dbDisconnect(con)
    if(saveThem){
    saveRDS(alldata, file.path(savePath, "ecology_card_vgis.rds"))
  } else if (!saveThem){
    return(alldata)
  } else{
    stop("saveThem must be logical.")
  }
}
