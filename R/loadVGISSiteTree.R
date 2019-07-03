#' Load the site/stump/small tree (3s) data from Oracle database
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
#' @rdname loadVGISSiteTree
#' @author Yong Luo
loadVGISSiteTree <- function(userName, password, saveThem = FALSE,
                             savePath = file.path(".")){
  drv <- dbDriver("Oracle")
  connect.string <-"(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)
  (HOST=nrk1-scan.bcgov)(PORT=1521))
  (CONNECT_DATA=(SERVER=DEDICATED)(SERVICE_NAME=ENVPROD1.NRS.BCGOV)))"
  con <- dbConnect(drv, username = userName, password = password,
                   dbname = connect.string)
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
  yr_cnt <- c10tree[,.(CLSTR_ID, PLOT, TREE_NO,
                       comp_meth = paste(COMPTYPE, "-", AS_METH, sep = ""),
                       AGE_METH, YR_CNT, CORELEN)]
  yr_cnt_wide <- yr_cnt[comp_meth != "RING-OCC" &  AGE_METH != "RANDOM",
                        .(CLSTR_ID, PLOT, TREE_NO, YR_CNT, comp_meth)]
  yr_cnt_wide <- yr_cnt_wide[!duplicated(yr_cnt_wide)]
  yr_cnt_wide <- reshape(data = yr_cnt_wide,
                         v.names = "YR_CNT",
                         timevar = "comp_meth",
                         idvar = c("CLSTR_ID", "PLOT", "TREE_NO"),
                         direction = "wide")
  setnames(yr_cnt_wide, paste("YR_CNT.",
                              c("RING-OFCOCC", "RING-FLDOCC", "REPRC-OCC",
                                "ADJ2GP-CALC", "TOTAL-CALC", "PHYSAGE-PRORATE"),
                              sep = ""),
           c("BORE_AGE", "BORAG_FL", "PRO_RING",
             "AGE_CORR", "TOTAL_AG", "PHYS_AGE"))
  alltrees <- merge(alltrees, yr_cnt_wide,
                    by = c("CLSTR_ID", "PLOT", "TREE_NO"),
                    all.x = TRUE)
  rm(yr_cnt_wide)

  growth <- yr_cnt[comp_meth == "RING-OCC" & AGE_METH != "RANDOM",
                   .(CLSTR_ID, PLOT, TREE_NO, YR_CNT, CORELEN)]
  growth <- growth[!duplicated(growth),]
  growth_wide <- reshape(data = growth,
                         v.names = "CORELEN",
                         timevar = "YR_CNT",
                         idvar = c("CLSTR_ID", "PLOT", "TREE_NO"),
                         direction = "wide")
  setnames(growth_wide, paste("CORELEN.", c(5, 10, 20), sep = ""),
           paste("GROW_", c(5, 10, 20), "YR", sep = ""))
  alltrees <- merge(alltrees, growth_wide,
                    by = c("CLSTR_ID", "PLOT", "TREE_NO"),
                    all.x = TRUE)
  rm(growth_wide, growth)

  pro_len <- yr_cnt[comp_meth == "REPRC-OCC" & AGE_METH != "RANDOM",
                    .(CLSTR_ID, PLOT, TREE_NO, PRO_LEN = CORELEN)]
  pro_len <- pro_len[!duplicated(pro_len),]
  alltrees <- merge(alltrees, pro_len,
                    by = c("CLSTR_ID", "PLOT", "TREE_NO"),
                    all.x = TRUE)
  rm(pro_len, yr_cnt)
  alltrees[CR_CL == "1", CR_CL := "D"]
  alltrees[CR_CL == "2", CR_CL := "C"]
  alltrees[CR_CL == "3", CR_CL := "I"]
  alltrees[CR_CL == "4", CR_CL := "S"]
  treeheight <- c10tree[HT_POS == "TOP",.(CLSTR_ID, PLOT, TREE_NO, TREE_LEN = HEIGHT)]
  treeheight <- treeheight[!duplicated(treeheight),]
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
