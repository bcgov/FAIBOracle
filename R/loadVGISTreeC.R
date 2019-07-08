#' Load the full/enhanced/H-enhanced tree data from Oracle database
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
#' @rdname loadVGISTreeC
#' @author Yong Luo
loadVGISTreeC <- function(userName, passWord, saveThem = FALSE,
                               savePath = file.path(".")){
  drv <- dbDriver("Oracle")
  connect.string <-"(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)
  (HOST=nrk1-scan.bcgov)(PORT=1521))
  (CONNECT_DATA=(SERVER=DEDICATED)(SERVICE_NAME=ENVPROD1.NRS.BCGOV)))"
  con <- dbConnect(drv, username = userName, password = passWord,
                   dbname = connect.string)
  c8tree <- dbGetQuery(con, "SELECT
                       vgis.vgis_projects.PROJECT_SKEY        AS PROJ_KEY,
                       vgis.vgis_projects.PROJECT_BUSINESS_ID      AS PROJ_ID,
                       vgis. PLOT_CLUSTERS.PLOT_CLUSTER_SKEY   AS CL_KEY,
                       vgis. PLOT_CLUSTERS.SAMPLE_EXP_PLOT_NUM AS SAMP_NO,
                       vgis. DESIGN_SPECIFICATIONS.BUSINESS_IDENTIFIER_VALUE AS TYPE_,
                       vgis. MEASMT_VISITATIONS.SAMPLE_INTENT  AS INTENT,
                       vgis. CLUSTER_ELEMENTS.PLOT_IDENTIFIER  AS PLOT_ID,
                       vgis. BIOMETRIC_DESIGN_SPECIFICATION.BD_SPEC_SKEY  AS SP_KEY,
                       vgis. MEASMT_VISITATIONS.MEASMT_VISIT_SKEY         AS VST_KEY,
                       vgis. MEASMT_VISITATIONS.MEASMT_VISITN_NUM         AS VISIT,
                       vgis. MEASMT_VISITATIONS.MEASMT_DATE    AS MEAS_DT,
                       vgis. SPATIAL_DELIMITERS.SPTLD_TYPE     AS X ,
                       vgis. SPATIAL_DELIMITERS.BASAL_AREA_FTR AS V_BAF,
                       vgis. SPATIAL_DELIMITERS.RADIUS         AS F_RAD,
                       vgis. POINT_LOCATION_VISITATIONS.CE_VISIT_SKEY     AS CVST_KEY,
                       vgis. POINT_LOCATION_VISITATIONS.PLOT_TYPE_CD      AS PL_TYPE,
                       vgis. POINT_LOCATION_VISITATIONS.PLOT_QUALIFIER_CD AS PL_QUAL,
                       vgis. TREES.TREE_NUM                    AS TREE_NUM,
                       vgis. TAXONOMIC_NAMES.LONG_CD          AS LONG_SPECIES,
                       vgis. TREE_MEASMTS.TREE_SELECTION_CD    AS TREETYPE,
                       vgis. TREE_MEASMTS.TREE_EXTANT_CD       AS L_D ,
                       vgis. TREE_MEASMTS.TREE_STANDING_IND    AS S_F,
                       vgis. TREE_MEASMTS.REMAINING_BARK_PCT   AS BARK_PER,
                       vgis. TREE_MEASMTS.CROWN_CLASS_CD       AS CR_CLSS,
                       vgis. DIAM_ASSMTS.MEASMT_HT             AS DIAM_HT,
                       vgis. DIAM_ASSMTS.DIAMETER              AS DIAM,
                       vgis. DIAM_ASSMTS.ASSMT_QUAL_CD         AS DIAM_QL,
                       vgis. DIAM_ASSMTS.POSITION_CD           AS DIAM_POS,
                       vgis.DIAM_ASSMTS.WALKTHROUGH_IND        as Walkthru_status,
                       vgis. HT_ASSMTS.HEIGHT                  AS HEIGHT,
                       vgis. HT_ASSMTS.ASSMT_QUAL_CD           AS HT_QUAL,
                       vgis. HT_ASSMTS.POSITION_CD             AS HT_POS,
                       vgis. TREES.SECTOR_NUM                  AS SECTOR,
                       vgis. TREES.RESIDUAL_IND                AS RESIDUAL,
                       vgis. MEASMT_VISITATIONS.PSTAT_PROCESS_STATE_CD as pstat_process_state_cd,
                       vgis. TREES.TREE_TYPE                   as tree_type

                       FROM
                       vgis. MEASMT_VISITATIONS,
                       vgis. PLOT_CLUSTERS,
                       vgis. POINT_LOCATION_VISITATIONS,
                       vgis. CLUSTER_ELEMENTS,
                       vgis. vgis_projects,
                       vgis. PROJECT_ASSOCIATIONS,
                       vgis. TREE_MEASMTS,
                       vgis. TREES,
                       vgis. GROUP_TAXON_MEMBERSHIPS,
                       vgis. TAXONOMIC_NAMES,
                       vgis. DIAM_ASSMTS,
                       vgis. HT_ASSMTS,
                       vgis. BIOMETRIC_DESIGN_SPECIFICATION,
                       vgis. SPATIAL_DELIMITERS,
                       vgis. DESIGN_SPECIFICATIONS
                       WHERE
                       /*( vgis. MEASMT_VISITATIONS.PSTAT_PROCESS_STATE_CD='DDL'
                       AND vgis. TREES.TREE_TYPE='IND')
                       AND*/  ((vgis. MEASMT_VISITATIONS.PCLTR_PLOT_CLUSTER_SKEY
                       =vgis. PLOT_CLUSTERS.PLOT_CLUSTER_SKEY)
                       AND (vgis. PLOT_CLUSTERS.DES_DESIGN_SKEY = vgis. DESIGN_SPECIFICATIONS.DESIGN_SKEY)
                       AND (vgis. POINT_LOCATION_VISITATIONS.MEAVT_MEASMT_VISIT_SKEY
                       =vgis. MEASMT_VISITATIONS.MEASMT_VISIT_SKEY)
                       AND (vgis. POINT_LOCATION_VISITATIONS.CELMT_CLUSTER_ELEMENT_SKEY
                       =vgis. CLUSTER_ELEMENTS.CLUSTER_ELEMENT_SKEY)
                       AND (vgis. PLOT_CLUSTERS.PLOT_CLUSTER_SKEY
                       =vgis. PROJECT_ASSOCIATIONS.PCLTR_PLOT_CLUSTER_SKEY)
                       AND (vgis. PROJECT_ASSOCIATIONS.PRJ_PROJECT_SKEY
                       =vgis.vgis_projects.PROJECT_SKEY)
                       AND (vgis. TREE_MEASMTS.CEV_CE_VISIT_SKEY
                       =vgis. POINT_LOCATION_VISITATIONS.CE_VISIT_SKEY)
                       AND (vgis. TREE_MEASMTS.TREE_TREE_SKEY
                       =vgis. TREES.TREE_SKEY)
                       AND (vgis. TREES.GTO_GROUP_TAXON_SKEY
                       =vgis. GROUP_TAXON_MEMBERSHIPS.GROUP_TAXON_SKEY)
                       AND (vgis. GROUP_TAXON_MEMBERSHIPS.TAXC_TAXONOMIC_CD_SKEY
                       =vgis. TAXONOMIC_NAMES.TAXONOMIC_CD_SKEY)
                       AND (vgis. DIAM_ASSMTS.TRMEA_TREE_MEASMT_SKEY
                       =vgis. TREE_MEASMTS.TREE_MEASMT_SKEY)
                       AND (vgis. HT_ASSMTS.TRMEA_TREE_MEASMT_SKEY
                       =vgis. TREE_MEASMTS.TREE_MEASMT_SKEY)
                       AND (vgis. CLUSTER_ELEMENTS.BDSPEC_BD_SPEC_SKEY
                       =vgis. BIOMETRIC_DESIGN_SPECIFICATION.BD_SPEC_SKEY)
                       AND (vgis. BIOMETRIC_DESIGN_SPECIFICATION.SPTLD_SPATIAL_DELIM_SKEY
                       =vgis. SPATIAL_DELIMITERS.SPATIAL_DELIM_SKEY))") %>%
    data.table
  ## the below part removes LGMW plots
  set(c8tree, , c("CL_KEY", "PROJ_KEY", "VST_KEY", "MEAS_DT", "SP_KEY",
                  "CVST_KEY", "X"), NULL)
  c8tree <-c8tree[PSTAT_PROCESS_STATE_CD == "DDL" & TREE_TYPE == "IND" &
                    PROJ_ID != "LGMW",]
  c8tree[,':='(CLSTR_ID = getClusterID(PROJ_ID, SAMP_NO, TYPE_,
                                       INTENT, VISIT),
               PLOT = getPlotCode(PLOT_ID),
               TREE_NO = getTreeID(TREE_NUM))]
  set(c8tree, , c("PROJ_ID", "SAMP_NO", "TYPE_", "INTENT",
                  "VISIT", "PLOT_ID", "TREE_NUM"), NULL)
  c8treelist <- unique(c8tree[DIAM_POS == "BOL" &
                                DIAM_HT == 1.3,
                              .(CLSTR_ID, PLOT, TREE_NO,
                                DBH = DIAM)],
                       by = c("CLSTR_ID", "PLOT",
                              "TREE_NO"))
  if(nrow(c8treelist) != nrow(unique(c8treelist, by = c("CLSTR_ID", "PLOT",
                                                        "TREE_NO")))){
    stop("check with Bob, multiple DBH records for a tree.")
  }
  totalnumtrees <- nrow(c8treelist)
  c8tree <- c8tree[!(duplicated(c8tree)),]
  diam_btp <- c8tree[DIAM_POS == "BRK", .(CLSTR_ID, PLOT, TREE_NO,
                                          DIAM_BTP = DIAM)]
  diam_btp <- diam_btp[!duplicated(diam_btp),]
  c8treelist <- merge(c8treelist, diam_btp,
                      by = c("CLSTR_ID", "PLOT", "TREE_NO"),
                      all = TRUE)
  if(nrow(c8treelist) != totalnumtrees){
    warning("check with Bob, multiple btop diam for a tree.")
  }
  rm(diam_btp)

  cr_class <- c8tree[,.(CLSTR_ID, PLOT, TREE_NO,
                        CR_CLSS)]
  cr_class[CR_CLSS == "1", CR_CL := "D"]
  cr_class[CR_CLSS == "2", CR_CL := "C"]
  cr_class[CR_CLSS == "3", CR_CL := "I"]
  cr_class[CR_CLSS == "4", CR_CL := "S"]
  cr_class[, CR_CLSS := NULL]
  cr_class <- cr_class[!duplicated(cr_class),]
  c8treelist <- merge(c8treelist, cr_class,
                      by = c("CLSTR_ID", "PLOT", "TREE_NO"),
                      all = TRUE)
  if(nrow(c8treelist) != totalnumtrees){
    warning("check with Bob, multiple crown class records for a tree.")
  }
  rm(cr_class)

  tree_len <- c8tree[HT_POS == "TOP", .(CLSTR_ID, PLOT, TREE_NO,
                                        TREE_LEN = HEIGHT)]
  tree_len <- tree_len[!duplicated(tree_len),]
  c8treelist <- merge(c8treelist, tree_len,
                      by = c("CLSTR_ID", "PLOT", "TREE_NO"),
                      all = TRUE)
  if(nrow(c8treelist) != totalnumtrees){
    warning("check with Bob, multiple tree heights for a tree.")
  }
  rm(tree_len)

  ht_proj <- c8tree[HT_POS == "PTP", .(CLSTR_ID, PLOT, TREE_NO,
                                       HT_PROJ = HEIGHT)]
  ht_proj <- ht_proj[!duplicated(ht_proj),]
  c8treelist <- merge(c8treelist, ht_proj,
                      by = c("CLSTR_ID", "PLOT", "TREE_NO"),
                      all = TRUE)
  if(nrow(c8treelist) != totalnumtrees){
    warning("check with Bob, multiple btop heights for a tree.")
  }
  rm(ht_proj)

  ht_brch <- c8tree[HT_POS == "LLB", .(CLSTR_ID, PLOT, TREE_NO,
                                       HT_BRCH = HEIGHT)]
  ht_brch <- ht_brch[!duplicated(ht_brch),]
  c8treelist <- merge(c8treelist, ht_brch,
                      by = c("CLSTR_ID", "PLOT", "TREE_NO"),
                      all = TRUE)
  if(nrow(c8treelist) != totalnumtrees){
    warning("check with Bob, multiple lowest live branch height for a tree.")
  }
  rm(ht_brch)

  s_f <- c8tree[, .(uniobs = 1:length(CLSTR_ID),
                    CLSTR_ID, PLOT, TREE_NO,
                    S_F, TREETYPE, HEIGHT)]
  removerows <- s_f[S_F %in% c(NA, " ") &
                      (!is.na(TREETYPE) | HEIGHT %in% c(0, NA)), ]$uniobs
  s_f <- s_f[!(uniobs %in% removerows),]
  rm(removerows)
  s_f[S_F %in% c(NA, " "), S_F := "S"]
  s_f[, ':='(uniobs = NULL,
             TREETYPE = NULL,
             HEIGHT = NULL)]
  s_f <- s_f[!duplicated(s_f),]
  c8treelist <- merge(c8treelist, s_f,
                      by = c("CLSTR_ID", "PLOT", "TREE_NO"),
                      all = TRUE)
  if(nrow(c8treelist) != totalnumtrees){
    warning("check with Bob, multiple lv_d records for a tree.")
  }
  rm(s_f)
  c8treelist[!is.na(DIAM_BTP) |
               !is.na(HT_PROJ), BRK_TOP := 1]
  c8treelist[is.na(BRK_TOP), BRK_TOP := 0]
  neededcols <- c8tree[,.(CLSTR_ID, PLOT, TREE_NO, LONG_SPECIES,
                          LV_D = substr(L_D, 1, 1), WALKTHRU_STATUS,
                          BARK_PER, SECTOR, RESIDUAL)]
  for(i in c("LONG_SPECIES", "LV_D", "WALKTHRU_STATUS", "BARK_PER", "SECTOR", "RESIDUAL")){
    neededcols[, newcol := neededcols[, i, with = FALSE]]
    neededcols[, newcollen := length(unique(newcol)),
               by = c("CLSTR_ID", "PLOT", "TREE_NO")]
    if(nrow(neededcols[newcollen > 1]) > 0){
      warning(paste("check with Bob, ", "multiple records of ", i, " found for each tree.", sep = ""))
    }
    set(neededcols, , c("newcol", "newcollen"), NULL)
  }
  rm(i)
  neededcols <- unique(neededcols,
                       by = c("CLSTR_ID", "PLOT", "TREE_NO"))

  c8treelist <- merge(c8treelist, neededcols,
                      by = c("CLSTR_ID", "PLOT", "TREE_NO"),
                      all = TRUE)

  rm(c8tree, neededcols)

  c8wild <- dbGetQuery(con, "SELECT  vgis.vgis_projects.PROJECT_SKEY                              AS PROJ_KEY,
                       vgis.vgis_projects.PROJECT_BUSINESS_ID                             AS PROJ_ID,
                       vgis. PLOT_CLUSTERS.PLOT_CLUSTER_SKEY                         AS CL_KEY,
                       vgis. PLOT_CLUSTERS.SAMPLE_EXP_PLOT_NUM                       AS SAMP_NO,
                       vgis. DESIGN_SPECIFICATIONS.BUSINESS_IDENTIFIER_VALUE         AS type_,
                       vgis. MEASMT_VISITATIONS.SAMPLE_INTENT                        AS INTENT,
                       vgis. CLUSTER_ELEMENTS.PLOT_IDENTIFIER                        AS PLOT_ID ,
                       vgis. MEASMT_VISITATIONS.MEASMT_VISIT_SKEY                    AS VST_KEY,
                       vgis. MEASMT_VISITATIONS.MEASMT_VISITN_NUM                    AS VISIT,
                       vgis. TREES.TREE_NUM                                          AS TREE_NUM,
                       vgis. TAXONOMIC_NAMES.LONG_CD                                AS LONG_SPECIES,
                       vgis. TREE_WILDLIFE_FTR_OCCURRENCES.TWFC_FEATURE_STATE_NUM    AS VALUE,
                       vgis. TREE_WILDLIFE_FTR_OCCURRENCES.TWFC_FEATURE_CATEGORY_CD  AS FEATURE,
                       vgis. TREE_WILDLIFE_USE_OCCURRENCES.TWUC_WILDLIFE_USE_CD      AS WILDLIFE
                       FROM
                       vgis. MEASMT_VISITATIONS,
                       vgis. PLOT_CLUSTERS,
                       vgis. POINT_LOCATION_VISITATIONS,
                       vgis. CLUSTER_ELEMENTS,
                       vgis.vgis_projects,
                       vgis. PROJECT_ASSOCIATIONS,
                       vgis. TREE_MEASMTS,
                       vgis. TREES,
                       vgis. TAXONOMIC_NAMES,
                       vgis. GROUP_TAXON_MEMBERSHIPS,
                       vgis. TREE_WILDLIFE_FTR_OCCURRENCES,
                       vgis. TREE_WILDLIFE_USE_OCCURRENCES,
                       vgis. DESIGN_SPECIFICATIONS
                       WHERE
                       (vgis. MEASMT_VISITATIONS.PSTAT_PROCESS_STATE_CD='DDL'
                       AND vgis. TREES.TREE_TYPE='IND')
                       AND  ((vgis. MEASMT_VISITATIONS.PCLTR_PLOT_CLUSTER_SKEY
                       =vgis. PLOT_CLUSTERS.PLOT_CLUSTER_SKEY)
                       AND (vgis. POINT_LOCATION_VISITATIONS.MEAVT_MEASMT_VISIT_SKEY
                       =vgis. MEASMT_VISITATIONS.MEASMT_VISIT_SKEY)
                       AND (vgis. POINT_LOCATION_VISITATIONS.CELMT_CLUSTER_ELEMENT_SKEY
                       =vgis. CLUSTER_ELEMENTS.CLUSTER_ELEMENT_SKEY)
                       AND (vgis. PLOT_CLUSTERS.PLOT_CLUSTER_SKEY
                       =vgis. PROJECT_ASSOCIATIONS.PCLTR_PLOT_CLUSTER_SKEY)
                       AND (vgis. PROJECT_ASSOCIATIONS.PRJ_PROJECT_SKEY
                       =vgis.vgis_projects.PROJECT_SKEY)
                       AND (vgis. TREE_MEASMTS.CEV_CE_VISIT_SKEY
                       =vgis. POINT_LOCATION_VISITATIONS.CE_VISIT_SKEY)
                       AND (vgis. TREE_MEASMTS.TREE_TREE_SKEY
                       =vgis. TREES.TREE_SKEY)
                       AND (vgis. TREES.GTO_GROUP_TAXON_SKEY
                       =vgis. GROUP_TAXON_MEMBERSHIPS.GROUP_TAXON_SKEY)
                       AND (vgis. GROUP_TAXON_MEMBERSHIPS.TAXC_TAXONOMIC_CD_SKEY
                       =vgis. TAXONOMIC_NAMES.TAXONOMIC_CD_SKEY)
                       AND (vgis. TREE_WILDLIFE_FTR_OCCURRENCES.TRMEA_TREE_MEASMT_SKEY
                       =vgis. TREE_MEASMTS.TREE_MEASMT_SKEY)
                       AND (vgis. TREE_WILDLIFE_USE_OCCURRENCES.TRMEA_TREE_MEASMT_SKEY(+)
                       =vgis. TREE_MEASMTS.TREE_MEASMT_SKEY)
                       AND (vgis. PLOT_CLUSTERS.DES_DESIGN_SKEY
                       =vgis. DESIGN_SPECIFICATIONS.DESIGN_SKEY))") %>%
    data.table
  set(c8wild, , c("CL_KEY", "PROJ_KEY", "VST_KEY"), NULL)
  c8wild[,':='(CLSTR_ID = getClusterID(PROJ_ID, SAMP_NO, TYPE_,
                                       INTENT, VISIT),
               PLOT = getPlotCode(PLOT_ID),
               TREE_NO = getTreeID(TREE_NUM))]
  set(c8wild, , c("PROJ_ID", "SAMP_NO", "TYPE_", "INTENT",
                  "VISIT", "PLOT_ID", "TREE_NUM"), NULL)

  c8wild[, FEATURE := toupper(FEATURE)]
  c8wild <- reshape(c8wild, v.names = "VALUE", timevar = "FEATURE",
                    idvar = c("CLSTR_ID", "PLOT", "TREE_NO"),
                    direction = "wide")
  c8wild <- c8wild[,.(CLSTR_ID, PLOT, TREE_NO, WL_USE = WILDLIFE,
                      WL_APPEA = VALUE.APPEARANCE,
                      WL_CROWN = VALUE.CROWN,
                      WL_BARK = VALUE.BARK,
                      WL_WOOD = VALUE.WOOD,
                      WL_LICHE = VALUE.LICHEN)]
  c8treelist <- merge(c8treelist, c8wild,
                      by = c("CLSTR_ID", "PLOT", "TREE_NO"),
                      all.x = TRUE)

  c8log <- dbGetQuery(con, "SELECT  vgis.vgis_projects.PROJECT_BUSINESS_ID        AS PROJ_ID,
                      vgis. PLOT_CLUSTERS.PLOT_CLUSTER_SKEY             AS CL_KEY,
                      vgis. PLOT_CLUSTERS.SAMPLE_EXP_PLOT_NUM           AS SAMP_NO,
                      vgis. CLUSTER_ELEMENTS.PLOT_IDENTIFIER            AS PLOT_ID,
                      vgis. DESIGN_SPECIFICATIONS.BUSINESS_IDENTIFIER_VALUE AS type_,
                      vgis. MEASMT_VISITATIONS.SAMPLE_INTENT            AS INTENT,
                      vgis. MEASMT_VISITATIONS.MEASMT_VISITN_NUM        AS VISIT,
                      vgis. TREE_MEASMTS.TREE_MEASMT_SKEY               AS MS_KEY,
                      vgis. TREES.TREE_NUM                              AS TREE_NUM,
                      vgis. TAXONOMIC_NAMES.LONG_CD                    AS LONG_SPECIES,
                      vgis. LOG_ASSESSMENTS.LAST_LOG_IND                AS LAST_IND,
                      vgis. LOG_ASSESSMENTS.SEQ_NUM                     AS SEQ_NO,
                      vgis. LOG_ASSESSMENTS.LENGTH                      AS LOG_LEN,
                      vgis. LOG_ASSESSMENTS.SOUND_PCT                   AS LOG_SND,
                      vgis. SPECIES_GRADE_CDS.LOG_GRADE_CD              AS LOG_GRD,
                      vgis. SPECIES_GRADE_CDS.LGC_DESC                  AS LOG_DESC
                      FROM
                      vgis. MEASMT_VISITATIONS,
                      vgis. PLOT_CLUSTERS,
                      vgis. POINT_LOCATION_VISITATIONS,
                      vgis. CLUSTER_ELEMENTS,
                      vgis.vgis_projects,
                      vgis. PROJECT_ASSOCIATIONS,
                      vgis. TREE_MEASMTS,
                      vgis. TREES,
                      vgis. TAXONOMIC_NAMES,
                      vgis. GROUP_TAXON_MEMBERSHIPS,
                      vgis. LOG_ASSESSMENTS,
                      vgis. SPECIES_GRADE_CDS,
                      vgis. DESIGN_SPECIFICATIONS
                      WHERE
                      (vgis. MEASMT_VISITATIONS.PSTAT_PROCESS_STATE_CD='DDL'
                      AND vgis. TREES.TREE_TYPE='IND')
                      AND  ((vgis. MEASMT_VISITATIONS.PCLTR_PLOT_CLUSTER_SKEY
                      =vgis. PLOT_CLUSTERS.PLOT_CLUSTER_SKEY)
                      AND (vgis. PLOT_CLUSTERS.DES_DESIGN_SKEY = vgis. DESIGN_SPECIFICATIONS.DESIGN_SKEY)
                      AND (vgis. POINT_LOCATION_VISITATIONS.MEAVT_MEASMT_VISIT_SKEY
                      =vgis. MEASMT_VISITATIONS.MEASMT_VISIT_SKEY)
                      AND (vgis. POINT_LOCATION_VISITATIONS.CELMT_CLUSTER_ELEMENT_SKEY
                      =vgis. CLUSTER_ELEMENTS.CLUSTER_ELEMENT_SKEY)
                      AND (vgis. PLOT_CLUSTERS.PLOT_CLUSTER_SKEY
                      =vgis. PROJECT_ASSOCIATIONS.PCLTR_PLOT_CLUSTER_SKEY)
                      AND (vgis. PROJECT_ASSOCIATIONS.PRJ_PROJECT_SKEY
                      =vgis.vgis_projects.PROJECT_SKEY)
                      AND (vgis. TREE_MEASMTS.CEV_CE_VISIT_SKEY
                      =vgis. POINT_LOCATION_VISITATIONS.CE_VISIT_SKEY)
                      AND (vgis. TREE_MEASMTS.TREE_TREE_SKEY
                      =vgis. TREES.TREE_SKEY)
                      AND (vgis. TREES.GTO_GROUP_TAXON_SKEY
                      =vgis. GROUP_TAXON_MEMBERSHIPS.GROUP_TAXON_SKEY)
                      AND (vgis. GROUP_TAXON_MEMBERSHIPS.TAXC_TAXONOMIC_CD_SKEY
                      =vgis. TAXONOMIC_NAMES.TAXONOMIC_CD_SKEY)
                      AND (vgis. LOG_ASSESSMENTS.TRMEA_TREE_MEASMT_SKEY
                      =vgis. TREE_MEASMTS.TREE_MEASMT_SKEY)
                      AND (vgis. LOG_ASSESSMENTS.LGC_LOG_GRADE_SKEY
                      =vgis. SPECIES_GRADE_CDS.LOG_GRADE_SKEY))
                      ORDER BY vgis. PLOT_CLUSTERS.SAMPLE_EXP_PLOT_NUM ASC,
                      vgis. TREES.TREE_NUM ASC") %>%
    data.table
  set(c8log, , c("CL_KEY", "MS_KEY"), NULL)
  c8log[,':='(CLSTR_ID = getClusterID(PROJ_ID, SAMP_NO, TYPE_,
                                      INTENT, VISIT),
              PLOT = getPlotCode(PLOT_ID),
              TREE_NO = getTreeID(TREE_NUM))]
  set(c8log, , c("PROJ_ID", "SAMP_NO", "TYPE_",
                 "INTENT", "VISIT", "PLOT_ID", "TREE_NUM"), NULL)
  c8log <- c8log[!duplicated(c8log),]
  c8log[, ':='(LAST_IND = NULL,
               LOG_DESC = NULL,
               LOG_GRD = substr(LOG_GRD, 1, 1))]
  c8log[, NO_LOGS := length(SEQ_NO),
        by = c("CLSTR_ID", "PLOT", "TREE_NO")]
  maxlength <- max(c8log$SEQ_NO)

  c8log <- reshape(c8log,
                   v.names = c("LOG_LEN", "LOG_SND", "LOG_GRD"),
                   timevar = "SEQ_NO",
                   idvar = c("CLSTR_ID", "PLOT", "TREE_NO"),
                   direction = "wide")
  setnames(c8log, paste("LOG_LEN.", 1:maxlength, sep = ""),
           paste("LOG", 1:maxlength, "_LEN", sep = ""))
  setnames(c8log, paste("LOG_GRD.", 1:maxlength, sep = ""),
           paste("LOG", 1:maxlength, "_GRD", sep = ""))
  setnames(c8log, paste("LOG_SND.", 1:maxlength, sep = ""),
           paste("LOG", 1:maxlength, "_SND", sep = ""))
  c8log <- c8log[, c("CLSTR_ID", "PLOT", "TREE_NO", "NO_LOGS",
                     paste("LOG", 1:maxlength, "_LEN", sep = ""),
                     paste("LOG", 1:maxlength, "_GRD", sep = ""),
                     paste("LOG", 1:maxlength, "_SND", sep = "")),
                 with = FALSE]
  c8treelist <- merge(c8treelist, c8log,
                      by = c("CLSTR_ID", "PLOT", "TREE_NO"),
                      all.x = TRUE)
  c8treelist[, LONG_SPECIES := toupper(LONG_SPECIES)]
  c8treelist[BRK_TOP == 1 & !is.na(NO_LOGS),
             NO_LOGS := NO_LOGS+1L]
  brknologs <- sort(unique(c8treelist[BRK_TOP == 1 & !is.na(NO_LOGS),]$NO_LOGS))
  for(i in brknologs){
    c8treelist[BRK_TOP == 1 & NO_LOGS == i,
               paste("LOG", i, "_GRD", sep = "") := "N"]
    c8treelist[BRK_TOP == 1 & NO_LOGS == i,
               paste("LOG", i, "_LEN", sep = "") := NA]
    c8treelist[BRK_TOP == 1 & NO_LOGS == i,
               paste("LOG", i, "_SND", sep = "") := 0]
  }
  c8treelist[is.na(NO_LOGS), needchangerows := TRUE]
  c8treelist[needchangerows == TRUE, ':='(NO_LOGS = 1,
                                  LOG1_GRD = "*", # flag for h enhanced trees
                                  LOG1_LEN = TREE_LEN,
                                  LOG1_SND = 100)]
  c8treelist[needchangerows == TRUE & BRK_TOP == 1,
             ':='(LOG2_GRD = "N",
                  LOG2_LEN = 0,
                  LOG2_SND = 0,
                  NO_LOGS = 2)]
  c8treelist[is.na(LV_D), LV_D := "L"]
  c8treelist[, ':='(BRK_TOP = NULL, needchangerows = NULL)]
  dbDisconnect(con)
  if(saveThem){
    saveRDS(c8treelist, file.path(savePath, "tree_cardc_vgis.rds"))
  } else if (!saveThem){
    return(c8treelist)
  } else{
    stop("saveThem must be logical.")
  }
}
