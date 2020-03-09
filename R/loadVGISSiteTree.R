#' Load the site/stump/small tree (3s) data from Oracle database
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
#' @importFrom data.table ':=' data.table dcast
#' @importFrom dplyr '%>%'
#' @importFrom ROracle dbConnect dbGetQuery dbDisconnect
#' @importFrom DBI dbDriver
#'
#' @rdname loadVGISSiteTree
#' @author Yong Luo
loadVGISSiteTree <- function(userName, passWord, saveThem = FALSE,
                             savePath = file.path(".")){
  drv <- dbDriver("Oracle")
  connect_string <- getServer(databaseName = "VGIS")
  con <- dbConnect(drv, username = userName, password = passWord,
                   dbname = connect_string)
  c10tree <- dbGetQuery(con, "SELECT ALL
                        vgis.vgis_projects.PROJECT_BUSINESS_ID                    as proj_id,
                        vgis.PLOT_CLUSTERS.SAMPLE_EXP_PLOT_NUM               as samp_no,
                        vgis.DESIGN_SPECIFICATIONS.BUSINESS_IDENTIFIER_VALUE as type_,
                        vgis.MEASMT_VISITATIONS.SAMPLE_INTENT                as intent,
                        vgis.MEASMT_VISITATIONS.MEASMT_VISITN_NUM            AS VISIT,
                        vgis.CLUSTER_ELEMENTS.PLOT_IDENTIFIER                as plot_id,
                        vgis.SPATIAL_DELIMITERS.SPTLD_TYPE                   as x,
                        vgis.SPATIAL_DELIMITERS.BASAL_AREA_FTR               as v_baf,
                        vgis.SPATIAL_DELIMITERS.RADIUS                       as f_rad,
                        vgis.POINT_LOCATION_VISITATIONS.PLOT_TYPE_CD         as pl_type,
                        vgis.POINT_LOCATION_VISITATIONS.PLOT_QUALIFIER_CD    as pl_qual,
                        vgis.TREES.TREE_NUM                                  as tree_num,
                        vgis.TAXONOMIC_NAMES.LONG_CD                        as LONG_SPECIES,
                        vgis.TREE_MEASMTS.CROWN_CLASS_CD                     as cr_clss,
                        vgis.HT_ASSMTS.HEIGHT                                as height,
                        vgis.HT_ASSMTS.ASSMT_QUAL_CD                         as ht_qual,
                        vgis.HT_ASSMTS.POSITION_CD                           as ht_pos,
                        vgis.HT_ASSMTS.REPRES_IND                            as ht_repr,
                        vgis.TREE_MEASMTS.BORED_QUALIFIER_CD                 as meas_cod,
                        vgis.AGE_ASSESSMENTS.ASSESSED_FEATURE                as as_featr,
                        vgis.AGE_ASSESSMENTS.MEASMT_HEIGHT                   as ms_ht,
                        vgis.AGE_ASSESSMENTS.BARK_THICKNESS                  as barkthck,
                        vgis.AGE_ASSESSMENTS.CORE_CONDITION_CD               as corecond,
                        vgis.AGE_ASSESSMENTS.DIAM_AT_BORING_HT               as diaborht,
                        vgis.AGE_ASSESSMENTS.REPRESENTATIVE_IND              as age_repr,
                        vgis.AGE_ASSMT_COMPONENTS.COMPONENT_TYPE_CD          as comptype,
                        vgis.AGE_ASSMT_COMPONENTS.ASSESSMENT_METHOD_CD       as as_meth,
                        vgis.AGE_ASSMT_COMPONENTS.YEAR_COUNT                 as yr_cnt,
                        vgis.AGE_ASSMT_COMPONENTS.CORE_SECTION_LENGTH        as corelen,
                        vgis.SELECTION_METHODS.METHOD_CD                     as age_meth
                        FROM
                        vgis.PLOT_CLUSTERS,
                        vgis.vgis_projects,
                        vgis.PROJECT_ASSOCIATIONS,
                        vgis.MEASMT_VISITATIONS,
                        vgis.POINT_LOCATION_VISITATIONS,
                        vgis.CLUSTER_ELEMENTS,
                        vgis.TREE_MEASMTS,
                        vgis.TREES,
                        vgis.TAXONOMIC_NAMES,
                        vgis.GROUP_TAXON_MEMBERSHIPS,
                        vgis.AGE_ASSESSMENTS,
                        vgis.AGE_ASSMT_COMPONENTS,
                        vgis.AUGMENTED_ASSMT_ACTIVITIES,
                        vgis.SELECTION_METHODS,
                        vgis.BIOMETRIC_DESIGN_SPECIFICATION,
                        vgis.SPATIAL_DELIMITERS,
                        vgis.HT_ASSMTS,
                        vgis.DESIGN_SPECIFICATIONS
                        WHERE
                        vgis.HT_ASSMTS.POSITION_CD='TOP'
                        AND((vgis.PROJECT_ASSOCIATIONS.PRJ_PROJECT_SKEY
                        =vgis.vgis_projects.PROJECT_SKEY)
                        AND (vgis.PROJECT_ASSOCIATIONS.PCLTR_PLOT_CLUSTER_SKEY
                        =vgis.PLOT_CLUSTERS.PLOT_CLUSTER_SKEY)
                        AND (vgis.MEASMT_VISITATIONS.PCLTR_PLOT_CLUSTER_SKEY
                        =vgis.PLOT_CLUSTERS.PLOT_CLUSTER_SKEY)
                        AND (vgis.POINT_LOCATION_VISITATIONS.MEAVT_MEASMT_VISIT_SKEY
                        =vgis.MEASMT_VISITATIONS.MEASMT_VISIT_SKEY)
                        AND (vgis.POINT_LOCATION_VISITATIONS.CELMT_CLUSTER_ELEMENT_SKEY
                        =vgis.CLUSTER_ELEMENTS.CLUSTER_ELEMENT_SKEY)
                        AND (vgis.TREE_MEASMTS.CEV_CE_VISIT_SKEY
                        =vgis.POINT_LOCATION_VISITATIONS.CE_VISIT_SKEY)
                        AND (vgis.TREE_MEASMTS.TREE_TREE_SKEY
                        =vgis.TREES.TREE_SKEY)
                        AND (vgis.TREES.GTO_GROUP_TAXON_SKEY
                        =vgis.GROUP_TAXON_MEMBERSHIPS.GROUP_TAXON_SKEY)
                        aND (vgis.GROUP_TAXON_MEMBERSHIPS.TAXC_TAXONOMIC_CD_SKEY
                        =vgis.TAXONOMIC_NAMES.TAXONOMIC_CD_SKEY)
                        AND (vgis.AGE_ASSESSMENTS.TRMEA_TREE_MEASMT_SKEY(+)
                        =vgis.TREE_MEASMTS.TREE_MEASMT_SKEY)
                        AND (vgis.AGE_ASSMT_COMPONENTS.AGEA_AGE_ASSMT_SKEY(+)
                        =vgis.AGE_ASSESSMENTS.AGE_ASSMT_SKEY)
                        AND (vgis.AUGMENTED_ASSMT_ACTIVITIES.AGEA_AGE_ASSMT_SKEY(+)
                        =vgis.AGE_ASSESSMENTS.AGE_ASSMT_SKEY)
                        AND (vgis.AUGMENTED_ASSMT_ACTIVITIES.SEL_SELECTION_SKEY
                        =vgis.SELECTION_METHODS.SELECTION_SKEY(+))
                        AND (vgis.CLUSTER_ELEMENTS.BDSPEC_BD_SPEC_SKEY
                        =vgis.BIOMETRIC_DESIGN_SPECIFICATION.BD_SPEC_SKEY)
                        AND (vgis.BIOMETRIC_DESIGN_SPECIFICATION.SPTLD_SPATIAL_DELIM_SKEY
                        =vgis.SPATIAL_DELIMITERS.SPATIAL_DELIM_SKEY)
                        AND (vgis.HT_ASSMTS.TRMEA_TREE_MEASMT_SKEY
                        =vgis.TREE_MEASMTS.TREE_MEASMT_SKEY)
                        AND (vgis.PLOT_CLUSTERS.DES_DESIGN_SKEY
                        =vgis.DESIGN_SPECIFICATIONS.DESIGN_SKEY))
                        ORDER BY
                        vgis.vgis_projects.PROJECT_BUSINESS_ID ASC,
                        vgis.PLOT_CLUSTERS.SAMPLE_EXP_PLOT_NUM ASC,
                        vgis.DESIGN_SPECIFICATIONS.BUSINESS_IDENTIFIER_VALUE ASC,
                        vgis.MEASMT_VISITATIONS.SAMPLE_INTENT ASC,
                        vgis.CLUSTER_ELEMENTS.PLOT_IDENTIFIER ASC,
                        vgis.TREES.TREE_NUM ASC") %>%
    data.table
  c10tree <- c10tree[!is.na(AGE_METH) & PROJ_ID != "LGMW",]
  c10tree[,':='(CLSTR_ID = getClusterID(PROJ_ID, SAMP_NO, TYPE_,
                                        INTENT, VISIT),
                PLOT = getPlotCode(PLOT_ID),
                TREE_NO = getTreeID(TREE_NUM))]
  set(c10tree, , c("PROJ_ID", "SAMP_NO", "TYPE_", "INTENT",
                   "VISIT", "PLOT_ID", "TREE_NUM", "X"), NULL)
  alltrees <- unique(c10tree[,.(CLSTR_ID, PLOT, TREE_NO,
                                LONG_SPECIES = toupper(LONG_SPECIES),
                                CR_CL = CR_CLSS)],
                     by = c("CLSTR_ID", "PLOT", "TREE_NO"))
  totaltreenum <- nrow(alltrees)



  yr_cnt <- data.table::copy(c10tree)
  yr_cnt[, comp_meth := paste0(COMPTYPE, "-", AS_METH)]
  boreage_table <- unique(yr_cnt[comp_meth == "RING-OFCOCC",
                                 .(CLSTR_ID, PLOT, TREE_NO,
                                   BORE_AGE = YR_CNT)],
                          by = c("CLSTR_ID", "PLOT", "TREE_NO"))
  alltrees <- merge(alltrees, boreage_table,
                    by = c("CLSTR_ID", "PLOT", "TREE_NO"),
                    all.x = TRUE)

  borag_fl_table <- unique(yr_cnt[comp_meth == "RING-FLDOCC",
                                  .(CLSTR_ID, PLOT, TREE_NO,
                                    BORAG_FL = YR_CNT)],
                           by = c("CLSTR_ID", "PLOT", "TREE_NO"))

  alltrees <- merge(alltrees, borag_fl_table,
                    by = c("CLSTR_ID", "PLOT", "TREE_NO"),
                    all.x = TRUE)
  ## there are duplicates for each
  ## disk
  growth_table <- unique(yr_cnt[comp_meth == "RING-OCC",
                                .(CLSTR_ID, PLOT, TREE_NO,
                                  YR_CNT, CORELEN)])

  growth_table <- data.table::dcast(data = growth_table,
                                    CLSTR_ID + PLOT + TREE_NO ~ YR_CNT,
                                    fun.aggregate = min,
                                    value.var = "CORELEN")

  setnames(growth_table, c("5", "10", "20"),
           c("GROW_5YR", "GROW_10Y", "GROW_20Y"))

  growth_table[GROW_5YR == Inf, GROW_5YR := NA]
  growth_table[GROW_10Y == Inf, GROW_10Y := NA]
  growth_table[GROW_20Y == Inf, GROW_20Y := NA]
  alltrees <- merge(alltrees, growth_table,
                    by = c("CLSTR_ID", "PLOT", "TREE_NO"),
                    all.x = TRUE)
  prorate <- unique(yr_cnt[comp_meth == "REPRC-OCC",
                           .(CLSTR_ID, PLOT, TREE_NO,
                             PRO_RING = YR_CNT,
                             PRO_LEN = CORELEN)],
                    by = c("CLSTR_ID", "PLOT", "TREE_NO"))
  alltrees <- merge(alltrees, prorate,
                    by = c("CLSTR_ID", "PLOT", "TREE_NO"),
                    all.x = TRUE)
  age_corr <- unique(yr_cnt[comp_meth == "ADJ2GP-CALC",
                            .(CLSTR_ID, PLOT, TREE_NO,
                              AGE_CORR = YR_CNT)],
                     by = c("CLSTR_ID", "PLOT", "TREE_NO"))
  alltrees <- merge(alltrees, age_corr,
                    by = c("CLSTR_ID", "PLOT", "TREE_NO"),
                    all.x = TRUE)
  totalage <- unique(yr_cnt[comp_meth == "TOTAL-CALC",
                            .(CLSTR_ID, PLOT, TREE_NO,
                              TOTAL_AG = YR_CNT)],
                     by = c("CLSTR_ID", "PLOT", "TREE_NO"))
  alltrees <- merge(alltrees, totalage,
                    by = c("CLSTR_ID", "PLOT", "TREE_NO"),
                    all.x = TRUE)
  physage <- unique(yr_cnt[comp_meth == "PHYSAGE-PRORATE",
                           .(CLSTR_ID, PLOT, TREE_NO,
                             PHYS_AGE = YR_CNT)],
                    by = c("CLSTR_ID", "PLOT", "TREE_NO"))
  alltrees <- merge(alltrees, physage,
                    by = c("CLSTR_ID", "PLOT", "TREE_NO"),
                    all.x = TRUE)

  alltrees[CR_CL == "1", CR_CL := "D"]
  alltrees[CR_CL == "2", CR_CL := "C"]
  alltrees[CR_CL == "3", CR_CL := "I"]
  alltrees[CR_CL == "4", CR_CL := "S"]
  treeheight <- unique(c10tree[HT_POS == "TOP",.(CLSTR_ID, PLOT, TREE_NO, TREE_LEN = HEIGHT)],
                       by = c("CLSTR_ID", "PLOT", "TREE_NO"))
  alltrees <- merge(alltrees, treeheight,
                    by = c("CLSTR_ID", "PLOT", "TREE_NO"),
                    all.x = TRUE)
  if(totaltreenum != nrow(alltrees)){
    warning("age tree may have multiple heights.")
  }
  treetype <- c10tree[,.(CLSTR_ID, PLOT, TREE_NO, AGE_METH,
                         TREETYPE = substr(AGE_METH, 1, 1))]
  treetype[AGE_METH == "OUTPTSUB", TREETYPE := "X"]
  treetype[AGE_METH == "INPTSUB", TREETYPE := "O"]
  treetype <- treetype[!duplicated(treetype),]
  alltrees <- merge(alltrees, treetype[,.(CLSTR_ID, PLOT, TREE_NO, TREETYPE)],
                    by = c("CLSTR_ID", "PLOT", "TREE_NO"),
                    all.x = TRUE)
  rm(treetype)

  alltrees <- merge(alltrees,
                    unique(c10tree[,.(CLSTR_ID, PLOT, TREE_NO,
                                      BNG_DIAM = DIABORHT,
                                      BARK_THK = BARKTHCK,
                                      SUIT_TR = AGE_REPR,
                                      BORED_HT = MS_HT,
                                      SUIT_HT = HT_REPR)],
                           by = c("CLSTR_ID", "PLOT", "TREE_NO")),
                    by = c("CLSTR_ID", "PLOT", "TREE_NO"),
                    all.x = TRUE)


  dbDisconnect(con)

  if(saveThem){
    saveRDS(alltrees, file.path(savePath, "siteTree_card_vgis.rds"))
  } else if (!saveThem){
    return(alltrees)
  } else{
    stop("saveThem must be logical.")
  }
}
