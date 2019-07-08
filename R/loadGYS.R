#' Load the data from GYS Oracle database
#'
#'
#' @description This function is to load the natural growth PSP data from GYS Oracle database. This function
#'              is adapted from the first part of \code{cv_gys.sas}.
#'
#' @param userName character, Specifies a valid user name in GYS Oracle database.
#' @param passWord character, Specifies the password to the user name.
#' @param savePath character, Specifies the path to save your outputs, you do not need to
#'                 specify if \code{saveThem} is turned off. If missing, the current working
#'                 directory will be choosed.
#'
#' @return no value returned
#'
#' @importFrom data.table ':=' data.table
#' @importFrom dplyr '%>%'
#' @importFrom ROracle dbConnect dbGetQuery dbDisconnect
#' @importFrom DBI dbDriver
#'
#' @rdname loadGYS
#' @author Yong Luo
loadGYS <- function(userName, passWord, savePath = file.path(".")){
  drv <- dbDriver("Oracle")
  connect.string <-"(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)
  (HOST=nrk1-scan.bcgov)(PORT=1521))
  (CONNECT_DATA=(SERVER=DEDICATED)(SERVICE_NAME=DBP07.NRS.BCGOV)))"
  con <- dbConnect(drv, username = userName, password = passWord,
                   dbname = connect.string)

  rcl_area_validn_codes <- dbGetQuery(con, "Select rcl_area_validn_skey ,
                                      inv_region_num  as region,
                                      compt_num as COMPT ,
                                      compt_ltr ,
                                      fiz_cd as fiz
                                      from rcl_area_validn_codes") %>%
    data.table
  saveRDS(rcl_area_validn_codes, file.path(savePath, "rcl_area_validn_codes.rds"))
  rcl_area_validn_codes <- rcl_area_validn_codes[,.(RCL_AREA_VALIDN_SKEY, COMPT_LTR, COMPT, REGION)]
  gc()

  gys_sample <- dbGetQuery(con, "SELECT    SAMPLE_ID   ,
                           bec_code_id ,
                           BEC_SITE_SERIES_ID,
                           BEC_SOURCE_CODE,
                           horizontal_datum_code,
                           bcgs_10k_map_Tile,
                           bcgs_20k_map_Tile,
                           ORG_UNIT_NO,
                           POSITION_METHOD_CODE,
                           SAMPLE_CONTACT_ID,
                           SAMPLE_STATUS_CODE,
                           SAMPLING_METHOD_CODE,
                           SPECIAL_CRUISE_SUBCODE  ,
                           SPECIAL_SAMPLE_CODE  ,
                           STAND_ORIGIN_CODE,
                           TSA_NUMBER_CODE as tsa_no,
                           TSB_NUMBER_CODE as tsa_blk,
                           DATA_OWNER_CODE,
                           VALIDATION_LEVEL_CODE,
                           RCL_AREA_VALIDN_SKEY,
                           SAMPLE_TYPE_CODE,
                           INSTALLATION,
                           SAMPLE_NO,
                           UTM_ZONE,
                           UTM_EASTING,
                           UTM_NORTHING,
                           FEATURE_CLASS_SKEY,
                           TIEPOINT_UTM_EASTING,
                           TIEPOINT_UTM_NORTHING,
                           STEM_MAPPED_IND,
                           TREATED_STAND_PLANTATION_YR,
                           TREATED_STAND_AGE_OF_STOCK,
                           BGC_ZONE_CODE  AS BGC_ZONE,
                           BGC_SUBZONE_CODE AS BGC_SBZN,
                           VARIANT AS BGC_VAR,
                           PHASE AS BGC_PHASE,
                           bec_site_series_cd,
                           ecosystem_subplot_phase,
                           bgc_transition_ind,
                           soil_moist_regime_dom,
                           soil_moist_regime_sub_dom,
                           soil_moist_regime_range,
                           soil_nutrient_regime_single,
                           site_series_proportion_of_plot,
                           SPECIAL_PROGRAM_IND,
                           REGIONAL_PROJECT_NO,
                           ESTABLISHMENT_YR,
                           PLOTS_IN_SAMPLE_CNT,
                           PER_HECTARE_FACTOR_1,
                           COMPANY_SAMPLE_ID_CROSS_REF,
                           TFL,
                           OPENING_NO,
                           site_series_phase_cd,
                           SPECIAL_CRUISE_NO,
                           FREQUENT_VALUE_ADJ_IND,
                           entry_timestamp,
                           entry_userid,
                           update_timestamp,
                           update_userid
                           FROM GYS_SAMPLE") %>%
    data.table

  gys_sample <- merge(gys_sample, rcl_area_validn_codes, by = "RCL_AREA_VALIDN_SKEY")
  gys_sample[, ST := substr(SAMPLE_TYPE_CODE, 1, 1)]
  gys_sample[is.na(INSTALLATION), INSTALLATION := 0]
  gys_sample[, ':='(REGION = sprintf("%02d", REGION),
                      COMPT = sprintf("%03d", COMPT),
                      INSTALLATION = sprintf("%03d", INSTALLATION),
                      SAMPLE_NO = sprintf("%03d", SAMPLE_NO))]
  gys_sample[is.na(COMPT_LTR), COMPT_LTR := " "]
  gys_sample[is.na(ST), ST := " "]
  gys_sample[, SAMP_ID := paste0(REGION, COMPT, COMPT_LTR,
                                   ST, INSTALLATION, SAMPLE_NO)]
  gys_sample <- gys_sample[substr(SAMP_ID, 7, 7) %in% c("G", "R", "T", "I")]
  sample_id_lookup <- unique(gys_sample[,.(SAMPLE_ID, SAMP_ID)],
                             by = c("SAMPLE_ID", "SAMP_ID"))
  gys_sample[, SAMPLE_ID := NULL]
  saveRDS(gys_sample, file.path(savePath, "gys_sample.rds"))
  rm(gys_sample)
  gc()

  gys_sample_measurement <- dbGetQuery(con, "SELECT * FROM GYS_SAMPLE_MEASUREMENT") %>%
    data.table
  gys_sample_measurement[,':='(MEAS_NO = sprintf("%02d", MEAS_NO))]
  gys_sample_measurement <- merge(gys_sample_measurement, sample_id_lookup,
                                  by = "SAMPLE_ID")
  gys_sample_measurement <- gys_sample_measurement[SAMP_ID %in% unique(sample_id_lookup$SAMP_ID),]

  gys_sample_measurement <- gys_sample_measurement[order(SAMP_ID, MEAS_NO),]

  meas_id_lookup <- unique(gys_sample_measurement[,.(SAMPLE_MEASUREMENT_ID, SAMP_ID, MEAS_NO)],
                           by = c("SAMPLE_MEASUREMENT_ID", "SAMP_ID", "MEAS_NO"))
  gys_sample_measurement[, SAMPLE_ID := NULL]

  saveRDS(gys_sample_measurement, file.path(savePath, "gys_sample_measurement.rds"))
  rm(gys_sample_measurement)
  gc()

  gys_plot <- dbGetQuery(con, "SELECT * FROM GYS_PLOT") %>%
    data.table
  gys_plot <- merge(gys_plot, sample_id_lookup,
                    by = "SAMPLE_ID")
  # nrow(gys_plot) == nrow(unique(gys_plot, by = c("SAMP_ID", "PLOT_NO")))
  # must be true
  gys_plot[, PLOT_NO := sprintf("%02d", PLOT_NO)]
  plot_id_lookup <- unique(gys_plot[,.(SAMP_ID, PLOT_NO, PLOT_ID)],
                           by = c("SAMP_ID", "PLOT_NO", "PLOT_ID"))
  gys_plot[, SAMPLE_ID := NULL]
  saveRDS(gys_plot, file.path(savePath, "gys_plot.rds"))
  rm(gys_plot)
  gc()

  gys_subplot <- dbGetQuery(con, "SELECT * FROM GYS_SUBPLOT") %>%
    data.table
  gys_subplot <- merge(gys_subplot, plot_id_lookup, by = "PLOT_ID")
  gys_subplot[, SAMP_ID := NULL]
  gys_subplot <- merge(gys_subplot, meas_id_lookup, by = "SAMPLE_MEASUREMENT_ID")
  gys_subplot <- gys_subplot[order(SAMP_ID, PLOT_NO, SUBPLOT_ID, MEAS_NO),]

  gys_subplot[,':='(SAMPLE_MEASUREMENT_ID = NULL,
                    PLOT_ID = NULL)]
  saveRDS(gys_subplot, file.path(savePath, "gys_subplot.rds"))
  rm(gys_subplot)
  gc()

  gys_tree <- dbGetQuery(con, "SELECT * FROM GYS_TREE") %>%
    data.table
  gys_tree <- merge(gys_tree, plot_id_lookup,
                    by = "PLOT_ID")
  gys_tree[, ':='(TREE_NO = sprintf("%04s", TREE_NO))]
  gc()
  tree_id_lookup <- unique(gys_tree[,.(SAMP_ID, PLOT_NO, TREE_NO, TREE_ID)],
                           by = c("SAMP_ID", "PLOT_NO", "TREE_NO", "TREE_ID"))

  saveRDS(gys_tree, file.path(savePath, "gys_tree.rds"))
  rm(gys_tree)
  gc()

  gys_tree_measurement <- dbGetQuery(con,
                                     "SELECT TREE_ID,
                                      TREE_MEASUREMENT_ID,
                                      SAMPLE_MEASUREMENT_ID,
                                      CROWN_CLASS_CODE as CRN_CL,
                                      HEIGHT_SOURCE_CODE,
                                      HT_DIAMETER_CURVE_USE_CODE,
                                      HT_MEASUREMENT_STATUS_CODE as HT_MEAS_STATUS,
                                      PITH_CODE as PITH,
                                      TREE_CLASS_CODE as TR_CLASS,
                                      DIAM_AT_13M as DBH,
                                      SITE_INDEX as SI_BHA50,
                                      CURVE_OR_EST_HT,
                                      SUB_PLOT_TREE_IND as SPL_TRE,
                                      DIAM_AT_137M as DM_137_4,
                                      MEAS_HT as HT_MEAS,
                                      HT_TO_BREAK as HT_BRK,
                                      BORING_HT as HT_BORE,
                                      BORING_AGE as AGE_BORE,
                                      NEW_AGE,
                                      STUMP_DIAM,
                                      STUMP_HT,
                                      STUMP_STATUS_CODE,
                                      TREE_SUITABLE_FOR_HT,
                                      AGE_CORRECTION as AGE_CORR,
                                      TOTAL_AGE as AGE_TOT,
                                      COMPILATION_AGE_IND asAGE_COMP_IND
                                      FROM GYS_TREE_MEASUREMENT") %>%
    data.table
  gc()

  gys_tree_measurement <- merge(gys_tree_measurement, tree_id_lookup,
                                by = "TREE_ID")
  gc()
  gys_tree_measurement[, SAMP_ID := NULL]
  gys_tree_measurement <- merge(gys_tree_measurement, meas_id_lookup,
                                by = "SAMPLE_MEASUREMENT_ID")
  gc()
  gys_tree_measurement[,':='(SAMPLE_MEASUREMENT_ID = NULL)]
  gys_tree_measurement <- gys_tree_measurement[substr(SAMP_ID, 7, 7) %in% c("G", "R", "T", "I"),]
  gc()
  tree_meas_id_lookup <- gys_tree_measurement[,.(TREE_MEASUREMENT_ID, SAMP_ID, PLOT_NO, TREE_NO, MEAS_NO)]
  saveRDS(gys_tree_measurement, file.path(savePath, "gys_tree_measurement.rds"))
  rm(gys_tree_measurement)
  gc()

  GYS_LAYER <- dbGetQuery(con, "SELECT * FROM GYS_LAYER") %>%
    data.table
  saveRDS(GYS_LAYER, file.path(savePath, "GYS_LAYER.rds"))
  rm(GYS_LAYER)
  gc()

  GYS_LAYER_SPECIES_ASSESSMENT <- dbGetQuery(con, "SELECT * FROM GYS_LAYER_SPECIES_ASSESSMENT") %>%
    data.table
  saveRDS(GYS_LAYER_SPECIES_ASSESSMENT, file.path(savePath, "GYS_LAYER_SPECIES_ASSESSMENT.rds"))
  rm(GYS_LAYER_SPECIES_ASSESSMENT)
  gc()

  gys_pathological_observation <- dbGetQuery(con, "SELECT * FROM gys_pathological_observation") %>%
    data.table
  gys_pathological_observation <- merge(gys_pathological_observation, tree_meas_id_lookup,
                                        by = "TREE_MEASUREMENT_ID",
                                        all.x = TRUE)
  gys_pathological_observation <- gys_pathological_observation[substr(SAMP_ID, 7, 7) %in% c("G", "R", "T", "I"),]
  gys_pathological_observation[, TREE_MEASUREMENT_ID := NULL]
  saveRDS(gys_pathological_observation, file.path(savePath, "gys_pathological_observation.rds"))
  rm(gys_pathological_observation)
  gc()

  gys_dead_tree_tally <- dbGetQuery(con, "SELECT * FROM GYS_DEAD_TREE_TALLY") %>%
    data.table
  gys_dead_tree_tally <- merge(gys_dead_tree_tally, plot_id_lookup,
                               by = "PLOT_ID", all.x = TRUE)
  gys_dead_tree_tally[,':='(PLOT_ID = NULL, SAMP_ID = NULL)]
  gys_dead_tree_tally <- merge(gys_dead_tree_tally, meas_id_lookup,
                               by = "SAMPLE_MEASUREMENT_ID", all.x = TRUE)
  gys_dead_tree_tally[, ':='(SAMPLE_MEASUREMENT_ID = NULL)]
  dead_tree_tally_id_lookup <- gys_dead_tree_tally[,.(DEAD_TREE_TALLY_ID, SAMP_ID, PLOT_NO, MEAS_NO)]
  saveRDS(gys_dead_tree_tally, file.path(savePath, "gys_dead_tree_tally.rds"))
  rm(gys_dead_tree_tally)
  gc()

  gys_silvcutrl_treatment <- dbGetQuery(con, "SELECT * FROM GYS_SILVCUTRL_TREATMENT") %>%
    data.table
  gys_silvcutrl_treatment <- merge(gys_silvcutrl_treatment, meas_id_lookup,
                                   by = "SAMPLE_MEASUREMENT_ID", all.x = TRUE)
  gys_silvcutrl_treatment[, SAMPLE_MEASUREMENT_ID := NULL]
  saveRDS(gys_silvcutrl_treatment, file.path(savePath, "gys_silvcutrl_treatment.rds"))
  rm(gys_silvcutrl_treatment)
  gc()

  gys_imperial_dbh_class_count <- dbGetQuery(con, "SELECT * FROM GYS_IMPERIAL_DBH_CLASS_COUNT") %>%
    data.table
  gys_imperial_dbh_class_count <- merge(gys_imperial_dbh_class_count, plot_id_lookup,
                                        by = "PLOT_ID", all.x = TRUE)
  gys_imperial_dbh_class_count[, ':='(SAMP_ID = NULL,
                                      PLOT_ID = NULL)]
  gys_imperial_dbh_class_count <- merge(gys_imperial_dbh_class_count, meas_id_lookup,
                                        by = "SAMPLE_MEASUREMENT_ID", all.x = TRUE)
  gys_imperial_dbh_class_count[, SAMPLE_MEASUREMENT_ID := NULL]
  saveRDS(gys_imperial_dbh_class_count, file.path(savePath, "gys_imperial_dbh_class_count.rds"))
  rm(gys_imperial_dbh_class_count)
  gc()

  gys_metric_dbh_class_count <- dbGetQuery(con, "SELECT * FROM GYS_METRIC_DBH_CLASS_COUNT") %>%
    data.table
  gys_metric_dbh_class_count <- merge(gys_metric_dbh_class_count, plot_id_lookup,
                                      by = "PLOT_ID", all.x = TRUE)
  gys_metric_dbh_class_count[, ':='(SAMP_ID = NULL,
                                    PLOT_ID = NULL)]
  gys_metric_dbh_class_count <- merge(gys_metric_dbh_class_count, meas_id_lookup,
                                      by = "SAMPLE_MEASUREMENT_ID", all.x = TRUE)
  gys_metric_dbh_class_count[, SAMPLE_MEASUREMENT_ID := NULL]
  saveRDS(gys_metric_dbh_class_count, file.path(savePath, "gys_metric_dbh_class_count.rds"))
  rm(gys_metric_dbh_class_count)
  gc()

  gys_access_note <- dbGetQuery(con, "SELECT * FROM GYS_access_note") %>%
    data.table
  gys_access_note <- merge(gys_access_note, sample_id_lookup, by = "SAMPLE_ID")
  gys_access_note <- gys_access_note[order(SAMP_ID),]
  gys_access_note <- gys_access_note[substr(SAMP_ID, 7, 7) %in% c("G", "R", "T", "I"),]
  saveRDS(gys_access_note, file.path(savePath, "gys_access_note.rds"))
  rm(gys_access_note)
  gc()
}
