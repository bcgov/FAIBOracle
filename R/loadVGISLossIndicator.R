#' Load the loss indicator data from Oracle database
#'
#'
#' @description This function is to load the loss indicator data from VGIS Oracle database. And
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
#' @rdname loadVGISLossIndicator
#' @author Yong Luo
loadVGISLossIndicator <- function(userName, passWord, saveThem = FALSE,
                                  savePath = file.path(".")){
  drv <- dbDriver("Oracle")
  connect_string <- getServer(databaseName = "VGIS")
  con <- dbConnect(drv, username = userName, password = passWord,
                   dbname = connect_string)
  c9loss <- dbGetQuery(con, "SELECT
                       vgis.PLOT_CLUSTERS.PLOT_CLUSTER_SKEY   as CL_KEY,
                       vgis.vgis_projects.PROJECT_SKEY             as PROJ_KEY,
                       vgis.vgis_projects.PROJECT_BUSINESS_ID      as  PROJ_ID,
                       vgis.PLOT_CLUSTERS.SAMPLE_EXP_PLOT_NUM as SAMP_NO,
                       vgis.DESIGN_SPECIFICATIONS.BUSINESS_IDENTIFIER_VALUE as TYPE_,
                       vgis.MEASMT_VISITATIONS.SAMPLE_INTENT  as  INTENT,
                       vgis.MEASMT_VISITATIONS.MEASMT_VISITN_NUM as VISIT,
                       vgis.CLUSTER_ELEMENTS.PLOT_IDENTIFIER     as PLOT_ID,
                       vgis.TREES.TREE_NUM                       as tree_num,
                       vgis.TAXONOMIC_NAMES.LONG_CD             as LONG_SPECIES,
                       vgis.TREE_MEASMTS.CROWN_CLASS_CD          as CR_clss,
                       vgis.TREE_VOLUME_LOSS_INDICATORS.FROM_POSITION as loc_from,
                       vgis.TREE_VOLUME_LOSS_INDICATORS.TO_POSITION as loc_to,
                       vgis.TREE_VOLUME_LOSS_INDICATORS.FREQUENCY as freq,
                       vgis.TREE_VOLUME_LOSS_INDICATORS.TVLC_TREE_LOSS_CD as loss_in,
                       vgis.TREE_VOLUME_LOSS_INDICATORS.TO_POSITION_QUALIFIER_CD as t_sign,
                       vgis.TREE_VOLUME_LOSS_INDICATORS.from_POSITION_QUALIFIER_CD as f_sign,
                       vgis.TREE_VOLUME_LOSS_INDICATORS.SEQUENCE_NUM as seq_no
                       FROM
                       vgis.MEASMT_VISITATIONS,
                       vgis.PLOT_CLUSTERS,
                       vgis.POINT_LOCATION_VISITATIONS,
                       vgis.vgis_projects,
                       vgis.PROJECT_ASSOCIATIONS,
                       vgis.CLUSTER_ELEMENTS,
                       vgis.TREE_MEASMTS,
                       vgis.TREES,
                       vgis.TAXONOMIC_NAMES,
                       vgis.GROUP_TAXON_MEMBERSHIPS,
                       vgis.TREE_VOLUME_LOSS_INDICATORS,
                       vgis.DESIGN_SPECIFICATIONS
                       WHERE (vgis.MEASMT_VISITATIONS.PSTAT_PROCESS_STATE_CD='DDL'
                       AND vgis.TREES.TREE_TYPE='IND')
                       AND  ((vgis.MEASMT_VISITATIONS.PCLTR_PLOT_CLUSTER_SKEY
                       =vgis.PLOT_CLUSTERS.PLOT_CLUSTER_SKEY)
                       AND (vgis.POINT_LOCATION_VISITATIONS.MEAVT_MEASMT_VISIT_SKEY
                       =vgis.MEASMT_VISITATIONS.MEASMT_VISIT_SKEY)
                       AND (vgis.PROJECT_ASSOCIATIONS.PCLTR_PLOT_CLUSTER_SKEY
                       =vgis.PLOT_CLUSTERS.PLOT_CLUSTER_SKEY)
                       AND (vgis.PROJECT_ASSOCIATIONS.PRJ_PROJECT_SKEY
                       =vgis.vgis_projects.PROJECT_SKEY)
                       AND (vgis.POINT_LOCATION_VISITATIONS.CELMT_CLUSTER_ELEMENT_SKEY
                       =vgis.CLUSTER_ELEMENTS.CLUSTER_ELEMENT_SKEY)
                       AND (vgis.TREE_MEASMTS.CEV_CE_VISIT_SKEY
                       =vgis.POINT_LOCATION_VISITATIONS.CE_VISIT_SKEY)
                       AND (vgis.TREE_MEASMTS.TREE_TREE_SKEY
                       =vgis.TREES.TREE_SKEY)
                       AND (vgis.TREES.GTO_GROUP_TAXON_SKEY
                       =vgis.GROUP_TAXON_MEMBERSHIPS.GROUP_TAXON_SKEY)
                       AND (vgis.GROUP_TAXON_MEMBERSHIPS.TAXC_TAXONOMIC_CD_SKEY
                       =vgis.TAXONOMIC_NAMES.TAXONOMIC_CD_SKEY)
                       AND (vgis.TREE_VOLUME_LOSS_INDICATORS.TRMEA_TREE_MEASMT_SKEY
                       =vgis.TREE_MEASMTS.TREE_MEASMT_SKEY)
                       AND (vgis.DESIGN_SPECIFICATIONS.DESIGN_SKEY
                       =vgis.PLOT_CLUSTERS.DES_DESIGN_SKEY))
                       ORDER BY
                       vgis.vgis_projects.PROJECT_BUSINESS_ID ASC,
                       vgis.PLOT_CLUSTERS.SAMPLE_EXP_PLOT_NUM ASC,
                       vgis.CLUSTER_ELEMENTS.PLOT_IDENTIFIER ASC,
                       vgis.TREES.TREE_NUM ASC,
                       vgis.TREE_VOLUME_LOSS_INDICATORS.FROM_POSITION ASC") %>%
    data.table
  set(c9loss, , c("CL_KEY", "PROJ_KEY"), NULL)
  c9loss[,':='(CLSTR_ID = getClusterID(PROJ_ID, SAMP_NO, TYPE_,
                                       INTENT, VISIT),
               PLOT = getPlotCode(PLOT_ID),
               TREE_NO = getTreeID(TREE_NUM))]
  c9loss <- c9loss[PROJ_ID != "LGMW",]
  set(c9loss, , c("PROJ_ID", "SAMP_NO", "TYPE_", "INTENT",
                  "VISIT", "PLOT_ID", "TREE_NUM"), NULL)

  c9loss <- c9loss[!duplicated(c9loss),]
  c9loss <- c9loss[order(CLSTR_ID, PLOT, TREE_NO, SEQ_NO),]
  c9loss[, newseq := 1:length(LOC_FROM),
         by = c("CLSTR_ID", "PLOT", "TREE_NO")]
  c9loss[, SEQ_NO := NULL]
  newseqmax <- max(c9loss$newseq)
  c9loss[T_SIGN == "A", T_SIGN := "+"]
  c9loss[T_SIGN == "B", T_SIGN := "-"]
  c9loss[F_SIGN == "A", F_SIGN := "+"]
  c9loss[F_SIGN == "B", F_SIGN := "-"]
  c9loss <- reshape(data = c9loss,
                    v.names = c("LOSS_IN", "LOC_FROM", "LOC_TO", "FREQ",
                                "T_SIGN", "F_SIGN"),
                    timevar = "newseq",
                    idvar = c("CLSTR_ID", "PLOT", "TREE_NO"),
                    direction = "wide")
  setnames(c9loss, paste("LOSS_IN.", 1:newseqmax, sep = ""),
           paste("LOSS", 1:newseqmax, "_IN", sep = ""))
  setnames(c9loss, paste("LOC_FROM.", 1:newseqmax, sep = ""),
           paste("LOC", 1:newseqmax, "_FRO", sep = ""))
  setnames(c9loss, paste("LOC_TO.", 1:newseqmax, sep = ""),
           paste("LOC", 1:newseqmax, "_TO", sep = ""))
  setnames(c9loss, paste("T_SIGN.", 1:newseqmax, sep = ""),
           paste("T_SIGN", 1:newseqmax, sep = ""))
  setnames(c9loss, paste("F_SIGN.", 1:newseqmax, sep = ""),
           paste("F_SIGN", 1:newseqmax, sep = ""))
  setnames(c9loss, paste("FREQ.", 1:newseqmax, sep = ""),
           paste("FREQ", 1:newseqmax, sep = ""))


  c9damg <- dbGetQuery(con, "SELECT
                       vgis.PLOT_CLUSTERS.PLOT_CLUSTER_SKEY        as  CL_KEY,
                       vgis.vgis_projects.PROJECT_SKEY                  as PROJ_KEY,
                       vgis.vgis_projects.PROJECT_BUSINESS_ID           as PROJ_ID,
                       vgis.PLOT_CLUSTERS.SAMPLE_EXP_PLOT_NUM      as SAMP_NO,
                       vgis.DESIGN_SPECIFICATIONS.BUSINESS_IDENTIFIER_VALUE as TYPE_,
                       vgis.MEASMT_VISITATIONS.SAMPLE_INTENT       as INTENT,
                       vgis.MEASMT_VISITATIONS.MEASMT_VISITN_NUM   as VISIT,
                       vgis.CLUSTER_ELEMENTS.PLOT_IDENTIFIER       as PLOT_ID,
                       vgis.TREES.TREE_NUM                         as tree_num,
                       vgis.TAXONOMIC_NAMES.LONG_CD               as LONG_SPECIES,
                       vgis.TREE_MEASMTS.CROWN_CLASS_CD            as CR_clss,
                       vgis.DAMAGE_OCCURRENCES.DISTURB_ASSMT       as assess ,
                       vgis.DAMAGE_OCCURRENCES.DAM_OCC_SKEY        as damg_key ,
                       vgis.DAMAGE_OCC_ATTR_VALUES.VALUE_RECORDED  as severity ,
                       vgis.DAMAGE_AGENT_VEG_SAM_VERSIONS.DAMAGE_AGENT_CD as  damg_old,
                       vgis.DAMAGE_AGENT_VEG_SAM_VERSIONS.DAST_CODE_ARGUMENT as  damg_new
                       FROM
                       vgis.MEASMT_VISITATIONS,
                       vgis.PLOT_CLUSTERS,
                       vgis.POINT_LOCATION_VISITATIONS,
                       vgis.vgis_projects,
                       vgis.PROJECT_ASSOCIATIONS,
                       vgis.CLUSTER_ELEMENTS,
                       vgis.TREE_MEASMTS,
                       vgis.TREES,
                       vgis.GROUP_TAXON_MEMBERSHIPS,
                       vgis.TAXONOMIC_NAMES,
                       vgis.DAMAGE_OCCURRENCES,
                       vgis.DAMAGE_OCC_ATTR_VALUES,
                       vgis.DAMAGE_AGENT_VEG_SAM_VERSIONS,
                       vgis.DESIGN_SPECIFICATIONS
                       WHERE (vgis.MEASMT_VISITATIONS.PSTAT_PROCESS_STATE_CD ='DDL'
                       AND vgis.TREES.TREE_TYPE ='IND')
                       AND  ((vgis.MEASMT_VISITATIONS.PCLTR_PLOT_CLUSTER_SKEY
                       =vgis.PLOT_CLUSTERS.PLOT_CLUSTER_SKEY)
                       AND (vgis.POINT_LOCATION_VISITATIONS.MEAVT_MEASMT_VISIT_SKEY=
                       vgis.MEASMT_VISITATIONS.MEASMT_VISIT_SKEY)
                       AND (vgis.PROJECT_ASSOCIATIONS.PCLTR_PLOT_CLUSTER_SKEY
                       =vgis.PLOT_CLUSTERS.PLOT_CLUSTER_SKEY)
                       AND (vgis.PROJECT_ASSOCIATIONS.PRJ_PROJECT_SKEY
                       =vgis.vgis_projects.PROJECT_SKEY)
                       AND (vgis.POINT_LOCATION_VISITATIONS.CELMT_CLUSTER_ELEMENT_SKEY
                       =vgis.CLUSTER_ELEMENTS.CLUSTER_ELEMENT_SKEY)
                       AND (vgis.TREE_MEASMTS.CEV_CE_VISIT_SKEY
                       =vgis.POINT_LOCATION_VISITATIONS.CE_VISIT_SKEY)
                       AND (vgis.TREE_MEASMTS.TREE_TREE_SKEY=vgis.TREES.TREE_SKEY)
                       AND (vgis.TREES.GTO_GROUP_TAXON_SKEY
                       =vgis.GROUP_TAXON_MEMBERSHIPS.GROUP_TAXON_SKEY)
                       AND (vgis.GROUP_TAXON_MEMBERSHIPS.TAXC_TAXONOMIC_CD_SKEY
                       =vgis.TAXONOMIC_NAMES.TAXONOMIC_CD_SKEY)
                       AND (vgis.DAMAGE_OCCURRENCES.TRMEA_TREE_MEASMT_SKEY
                       =vgis.TREE_MEASMTS.TREE_MEASMT_SKEY)
                       AND (vgis.DAMAGE_OCC_ATTR_VALUES.DMGO_DAM_OCC_SKEY(+)
                       =vgis.DAMAGE_OCCURRENCES.DAM_OCC_SKEY)
                       AND ((vgis.DAMAGE_OCCURRENCES.DAST_CODE_ARGUMENT
                       =vgis.DAMAGE_AGENT_VEG_SAM_VERSIONS.DAST_CODE_ARGUMENT)
                       OR (vgis.DAMAGE_OCCURRENCES.DAGNC_DAVSV_SKEY
                       =vgis.DAMAGE_AGENT_VEG_SAM_VERSIONS.DAVSV_SKEY))
                       AND (vgis.DESIGN_SPECIFICATIONS.DESIGN_SKEY
                       =vgis.PLOT_CLUSTERS.DES_DESIGN_SKEY ))
                       ORDER BY vgis.vgis_projects.PROJECT_BUSINESS_ID ASC,
                       vgis.PLOT_CLUSTERS.SAMPLE_EXP_PLOT_NUM ASC,
                       vgis.CLUSTER_ELEMENTS.PLOT_IDENTIFIER ASC,
                       vgis.TREES.TREE_NUM ASC") %>%
    data.table
  set(c9damg, , c("CL_KEY", "PROJ_KEY", "DAMG_KEY"), NULL)
  c9damg[,':='(CLSTR_ID = getClusterID(PROJ_ID, SAMP_NO, TYPE_,
                                       INTENT, VISIT),
               PLOT = getPlotCode(PLOT_ID),
               TREE_NO = getTreeID(TREE_NUM))]
  c9damg <- c9damg[PROJ_ID != "LGMW",]
  set(c9damg, , c("PROJ_ID", "SAMP_NO", "TYPE_", "INTENT",
                  "VISIT", "PLOT_ID", "TREE_NUM", "ASSESS"), NULL)
  c9damg <- c9damg[order(CLSTR_ID, PLOT, TREE_NO),]
  c9damg[, neworder := 1:length(DAMG_OLD),
         by = c("CLSTR_ID", "PLOT", "TREE_NO")]
  maxneworder <- max(c9damg$neworder)
  c9damg <- reshape(data = c9damg,
                         v.names = c("DAMG_NEW", "DAMG_OLD", "SEVERITY"),
                         timevar = "neworder",
                         idvar = c("CLSTR_ID", "PLOT", "TREE_NO"),
                         direction = "wide")
  setnames(c9damg, paste("DAMG_NEW.", 1:maxneworder, sep = ""),
           paste("DAM_AGN", LETTERS[1:maxneworder], sep = ""))
  setnames(c9damg, paste("DAMG_OLD.", 1:maxneworder, sep = ""),
           paste("OLD_AGN", LETTERS[1:maxneworder], sep = ""))
  setnames(c9damg, paste("SEVERITY.", 1:maxneworder, sep = ""),
           paste("SEV_", LETTERS[1:maxneworder], sep = ""))



  c9stem <- dbGetQuery(con, "SELECT
                       vgis.PLOT_CLUSTERS.PLOT_CLUSTER_SKEY as CL_KEY,
                       vgis.vgis_projects.PROJECT_SKEY  as proj_key,
                       vgis.vgis_projects.PROJECT_BUSINESS_ID as PROJ_ID,
                       vgis.PLOT_CLUSTERS.SAMPLE_EXP_PLOT_NUM as SAMP_NO,
                       vgis.DESIGN_SPECIFICATIONS.BUSINESS_IDENTIFIER_VALUE as TYPE_,
                       vgis.MEASMT_VISITATIONS.SAMPLE_INTENT as INTENT,
                       vgis.MEASMT_VISITATIONS.MEASMT_VISITN_NUM as VISIT,
                       vgis.CLUSTER_ELEMENTS.PLOT_IDENTIFIER as plot_id ,
                       vgis.MEASMT_VISITATIONS.MEASMT_VISIT_SKEY as v_key,
                       vgis.TREES.TREE_NUM as  tree_num,
                       vgis.TAXONOMIC_NAMES.LONG_CD as LONG_SPECIES,
                       vgis.TREES.X_COORD as x_coord,
                       vgis.TREES.Y_COORD as y_coord
                       FROM
                       vgis.MEASMT_VISITATIONS,
                       vgis.PLOT_CLUSTERS,
                       vgis.POINT_LOCATION_VISITATIONS,
                       vgis.CLUSTER_ELEMENTS,
                       vgis.vgis_projects,
                       vgis.PROJECT_ASSOCIATIONS,
                       vgis.TREE_MEASMTS,
                       vgis.TREES, vgis.TAXONOMIC_NAMES,
                       vgis.GROUP_TAXON_MEMBERSHIPS,
                       vgis.DESIGN_SPECIFICATIONS
                       WHERE (vgis.MEASMT_VISITATIONS.PSTAT_PROCESS_STATE_CD='DDL'
                       AND vgis.TREES.TREE_TYPE='IND')
                       AND  ((vgis.MEASMT_VISITATIONS.PCLTR_PLOT_CLUSTER_SKEY
                       =vgis.PLOT_CLUSTERS.PLOT_CLUSTER_SKEY)
                       AND (vgis.POINT_LOCATION_VISITATIONS.MEAVT_MEASMT_VISIT_SKEY
                       =vgis.MEASMT_VISITATIONS.MEASMT_VISIT_SKEY)
                       AND (vgis.POINT_LOCATION_VISITATIONS.CELMT_CLUSTER_ELEMENT_SKEY
                       =vgis.CLUSTER_ELEMENTS.CLUSTER_ELEMENT_SKEY)
                       AND (vgis.PROJECT_ASSOCIATIONS.PCLTR_PLOT_CLUSTER_SKEY
                       =vgis.PLOT_CLUSTERS.PLOT_CLUSTER_SKEY)
                       AND (vgis.PROJECT_ASSOCIATIONS.PRJ_PROJECT_SKEY
                       =vgis.vgis_projects.PROJECT_SKEY)
                       AND (vgis.TREE_MEASMTS.CEV_CE_VISIT_SKEY
                       =vgis.POINT_LOCATION_VISITATIONS.CE_VISIT_SKEY)
                       AND (vgis.TREE_MEASMTS.TREE_TREE_SKEY
                       =vgis.TREES.TREE_SKEY)
                       AND (vgis.TREES.GTO_GROUP_TAXON_SKEY
                       =vgis.GROUP_TAXON_MEMBERSHIPS.GROUP_TAXON_SKEY)
                       AND (vgis.GROUP_TAXON_MEMBERSHIPS.TAXC_TAXONOMIC_CD_SKEY
                       =vgis.TAXONOMIC_NAMES.TAXONOMIC_CD_SKEY)
                       AND (vgis.DESIGN_SPECIFICATIONS.DESIGN_SKEY
                       =vgis.PLOT_CLUSTERS.DES_DESIGN_SKEY ))
                       ORDER BY
                       vgis.vgis_projects.PROJECT_BUSINESS_ID,
                       vgis.PLOT_CLUSTERS.SAMPLE_EXP_PLOT_NUM,
                       vgis.PLOT_CLUSTERS.SAMPLE_TYPE_CD,
                       vgis.CLUSTER_ELEMENTS.PLOT_IDENTIFIER,
                       vgis.TREES.TREE_NUM") %>%
    data.table
  set(c9stem, , c("CL_KEY", "PROJ_KEY", "V_KEY"), NULL)
  c9stem <- c9stem[!is.na(X_COORD) & !is.na(Y_COORD) & PLOT_ID == 1,]
  c9stem[,':='(CLSTR_ID = getClusterID(PROJ_ID, SAMP_NO, TYPE_,
                                      INTENT, VISIT),
              PLOT = getPlotCode(PLOT_ID),
              TREE_NO = getTreeID(TREE_NUM))]
  c9stem <- c9stem[PROJ_ID != "LGMW",]
  set(c9stem, , c("PROJ_ID", "SAMP_NO", "TYPE_", "INTENT",
                  "VISIT", "PLOT_ID", "TREE_NUM"), NULL)
  c9stem <- c9stem[!duplicated(c9stem),]
  radian <- 360/(2*pi)
  c9stem[, ':='(DISTANCE = sqrt(X_COORD^2 + Y_COORD^2))]
  c9stem[X_COORD >= 0 & Y_COORD > 0, AZIMUTH := radian*asin(X_COORD/DISTANCE)]
  c9stem[X_COORD > 0 & Y_COORD <= 0, AZIMUTH := 90 + radian*asin(abs(Y_COORD)/DISTANCE)]
  c9stem[X_COORD <= 0 & Y_COORD < 0, AZIMUTH := 180 + radian*asin(abs(X_COORD)/DISTANCE)]
  c9stem[X_COORD < 0 & Y_COORD >= 0, AZIMUTH := 270 + radian*asin(abs(Y_COORD)/DISTANCE)]
  alltrees <- rbind(c9loss[,.(CLSTR_ID, PLOT, TREE_NO, LONG_SPECIES)],
                    c9damg[,.(CLSTR_ID, PLOT, TREE_NO, LONG_SPECIES)],
                    c9stem[,.(CLSTR_ID, PLOT, TREE_NO, LONG_SPECIES)])
  alltrees[, LONG_SPECIES := toupper(LONG_SPECIES)]
  alltrees <- unique(alltrees, by = c("CLSTR_ID", "PLOT", "TREE_NO"))
  c9loss[, ':='(LOSS = TRUE,
                LONG_SPECIES = NULL,
                CR_CLSS = NULL)]
  alltrees <- merge(alltrees, c9loss,
                    by = c("CLSTR_ID", "PLOT", "TREE_NO"),
                    all.x = TRUE)
  c9damg[, ':='(LONG_SPECIES = NULL,
                CR_CLSS = NULL,
                DAMG = TRUE)]
  alltrees <- merge(alltrees, c9damg,
                    by = c("CLSTR_ID", "PLOT", "TREE_NO"),
                    all.x = TRUE)
  c9stem[, ':='(LONG_SPECIES = NULL,
                X_COORD = NULL,
                Y_COORD = NULL,
                STEM = TRUE)]
  alltrees <- merge(alltrees, c9stem,
                    by = c("CLSTR_ID", "PLOT", "TREE_NO"),
                    all.x = TRUE)
  dbDisconnect(con)
  if(saveThem){
    saveRDS(alltrees, file.path(savePath, "lossIndicator_card_vgis.rds"))
  } else if (!saveThem){
    return(alltrees)
  } else{
    stop("saveThem must be logical.")
  }
}
