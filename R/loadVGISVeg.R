#' Load the veg data from Oracle database
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
#' @rdname loadVGISVeg
#' @author Yong Luo
loadVGISVeg <- function(userName, password, saveThem = FALSE,
                        savePath = file.path(".")){
  drv <- dbDriver("Oracle")
  connect.string <-"(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)
  (HOST=nrk1-scan.bcgov)(PORT=1521))
  (CONNECT_DATA=(SERVER=DEDICATED)(SERVICE_NAME=ENVPROD1.NRS.BCGOV)))"
  con <- dbConnect(drv, username = userName, password = password,
                   dbname = connect.string)
  c14veg <- dbGetQuery(con, "SELECT
                       vgis.PLOT_CLUSTERS.PLOT_CLUSTER_SKEY                      AS CL_KEY,
                       vgis.vgis_projects.PROJECT_SKEY                                AS PROJ_KEY,
                       vgis.vgis_projects.PROJECT_BUSINESS_ID                         AS PROJ_ID,
                       vgis.PLOT_CLUSTERS.SAMPLE_EXP_PLOT_NUM                    AS SAMP_NO,
                       vgis.DESIGN_SPECIFICATIONS.BUSINESS_IDENTIFIER_VALUE      AS TYPE_,
                       vgis.MEASMT_VISITATIONS.SAMPLE_INTENT                     AS INTENT,
                       vgis.MEASMT_VISITATIONS.MEASMT_VISITN_NUM                 AS VISIT,
                       vgis.VEGETATION_LYR_COVER_ASSMTS.FIELD_SEQ_NUM            AS ITEM_NUM,
                       vgis.TAXONOMIC_NAMES.LONG_CD                              AS SPECIES,
                       vgis.VEGETATION_LYR_COVER_ASSMTS.COVER_PCT                AS CVR_PCT,
                       vgis.SPECIES_GROUP_DEFINITIONS.GROUP_CD                   AS LIFE_GRP,
                       vgis.VEGETATION_LYR_COVER_ASSMTS.STRATA_AVERAGE_HT        AS AVG_HT,
                       vgis.VEGETATION_LYR_COVER_ASSMTS.VLC_VEGETATION_LAYER_CD  AS LYR_CD

                       FROM vgis.MEASMT_VISITATIONS,
                       vgis.PLOT_CLUSTERS,
                       vgis.POINT_LOCATION_VISITATIONS,
                       vgis.vgis_projects,
                       vgis.PROJECT_ASSOCIATIONS,
                       vgis.CLUSTER_ELEMENTS,
                       vgis.TAXONOMIC_NAMES,
                       vgis.GROUP_TAXON_MEMBERSHIPS,
                       vgis.SPECIES_GROUP_DEFINITIONS,
                       vgis.VEGETATION_LYR_COVER_ASSMTS,
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
                       AND (vgis.POINT_LOCATION_VISITATIONS.CELMT_CLUSTER_ELEMENT_SKEY
                       =vgis.CLUSTER_ELEMENTS.CLUSTER_ELEMENT_SKEY)
                       AND (vgis.PLOT_CLUSTERS.DES_DESIGN_SKEY
                       =vgis.DESIGN_SPECIFICATIONS.DESIGN_SKEY)
                       AND (vgis.GROUP_TAXON_MEMBERSHIPS.TAXC_TAXONOMIC_CD_SKEY
                       =vgis.TAXONOMIC_NAMES.TAXONOMIC_CD_SKEY)
                       AND (vgis.GROUP_TAXON_MEMBERSHIPS.SGDEF_SPP_GROUP_SKEY
                       =vgis.SPECIES_GROUP_DEFINITIONS.SPP_GROUP_SKEY)
                       AND (VEGETATION_LYR_COVER_ASSMTS.GTO_GROUP_TAXON_SKEY
                       =vgis.GROUP_TAXON_MEMBERSHIPS.GROUP_TAXON_SKEY)
                       AND (vgis.VEGETATION_LYR_COVER_ASSMTS.CEV_CE_VISIT_SKEY
                       =vgis.POINT_LOCATION_VISITATIONS.CE_VISIT_SKEY))
                       AND (vgis.SPECIES_GROUP_DEFINITIONS.SPP_GROUP_SKEY IN (
                       SELECT SGDEF_SPP_GROUP_SKEY
                       FROM SPECIES_GROUP_ASSOCIATIONS
                       CONNECT BY PRIOR SGDEF_SPP_GROUP_SKEY = SGDEF_SPP_GROUP_SKEY_PARENT
                       START WITH SGDEF_SPP_GROUP_SKEY_PARENT =
                       ( SELECT SPP_GROUP_SKEY FROM SPECIES_GROUP_DEFINITIONS
                       WHERE GROUP_CD = 'LIFEFORM' )))") %>%
    data.table
  set(c14veg, , c("CL_KEY", "PROJ_KEY"), NULL)
  c14veg[,':='(CLSTR_ID = getClusterID(PROJ_ID, SAMP_NO, TYPE_,
                                       INTENT, VISIT),
               PLOT = "I")]
  set(c14veg, , c("PROJ_ID", "SAMP_NO", "TYPE_", "INTENT", "VISIT"), NULL)
  c14veg[, ITEM_NUM := gsub(" ", "", ITEM_NUM)]
  c14veg[nchar(ITEM_NUM) == 1, ITEM_NUM := paste("0", ITEM_NUM, sep = "")]
  c14veg[nchar(ITEM_NUM) %in% c(0, NA), ITEM_NUM := "00"]
  setnames(c14veg, "ITEM_NUM", "ITEM_NO")
  c14veg2 <- c14veg[LYR_CD %in% c("B1", "B2", "A"),]
  c15veg2 <- c14veg[LYR_CD %in% c("C"),]
  c14veg2 <- rbind(c14veg2,
                   c14veg[(!(LYR_CD %in% c("B1", "B2", "A", "C")) | is.na(LYR_CD))
                          & LIFE_GRP == "TREE",])
  c15veg2 <- rbind(c15veg2,
                   c14veg[(!(LYR_CD %in% c("B1", "B2", "A", "C")) | is.na(LYR_CD))
                          & (LIFE_GRP != "TREE" | is.na(LIFE_GRP)),])



  c14hdr <- dbGetQuery(con, "SELECT
                       vgis.PLOT_CLUSTERS.PLOT_CLUSTER_SKEY                        AS CL_KEY,
                       vgis.vgis_projects.PROJECT_SKEY                                  AS PROJ_KEY,
                       vgis.vgis_projects.PROJECT_BUSINESS_ID                           AS PROJ_ID,
                       vgis.PLOT_CLUSTERS.SAMPLE_EXP_PLOT_NUM                      AS SAMP_NO,
                       vgis.DESIGN_SPECIFICATIONS.BUSINESS_IDENTIFIER_VALUE        AS TYPE_,
                       vgis.MEASMT_VISITATIONS.SAMPLE_INTENT                       AS INTENT,
                       vgis.MEASMT_VISITATIONS.MEASMT_VISITN_NUM                   AS VISIT,
                       vgis.SPATIAL_DELIMITERS.RADIUS                              AS RADIUS,
                       vgis.VEGETATION_LAYER_OVERALL_COVER.VLC_VEGETATION_LAYER_CD AS LYR_CD,
                       vgis.VEGETATION_LAYER_OVERALL_COVER.COVER_PCT               AS CVR_PCT

                       FROM vgis.MEASMT_VISITATIONS,
                       vgis.PLOT_CLUSTERS,
                       vgis.POINT_LOCATION_VISITATIONS,
                       vgis.vgis_projects,
                       vgis.PROJECT_ASSOCIATIONS,
                       vgis.CLUSTER_ELEMENTS,
                       vgis.VEGETATION_LAYER_OVERALL_COVER,
                       vgis.BIOMETRIC_DESIGN_SPECIFICATION,
                       vgis.SPATIAL_DELIMITERS,
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
                       AND (vgis.POINT_LOCATION_VISITATIONS.CELMT_CLUSTER_ELEMENT_SKEY
                       =vgis.CLUSTER_ELEMENTS.CLUSTER_ELEMENT_SKEY)
                       AND (vgis.VEGETATION_LAYER_OVERALL_COVER.CEV_CE_VISIT_SKEY
                       =vgis.POINT_LOCATION_VISITATIONS.CE_VISIT_SKEY)
                       AND (vgis.CLUSTER_ELEMENTS.BDSPEC_BD_SPEC_SKEY
                       =vgis.BIOMETRIC_DESIGN_SPECIFICATION.BD_SPEC_SKEY)
                       AND (vgis.BIOMETRIC_DESIGN_SPECIFICATION.SPTLD_SPATIAL_DELIM_SKEY
                       =vgis.SPATIAL_DELIMITERS.SPATIAL_DELIM_SKEY))") %>%
    data.table
  set(c14hdr, , c("CL_KEY", "PROJ_KEY"), NULL)
  c14hdr[,':='(CLSTR_ID = getClusterID(PROJ_ID, SAMP_NO, TYPE_,
                                       INTENT, VISIT),
               PLOT = "I")]
  set(c14hdr, , c("PROJ_ID", "SAMP_NO", "TYPE_", "INTENT", "VISIT"), NULL)
  c14hdr2 <- c14hdr[LYR_CD %in% c("B1", "B2", "A"),]
  c15hdr2 <- c14hdr[!(LYR_CD %in% c("B1", "B2", "A")) | is.na(LYR_CD),]
  cd_ehh <- reshape(data = c15hdr2,
                    v.names = "CVR_PCT",
                    timevar = "LYR_CD",
                    idvar = "CLSTR_ID",
                    direction = "wide")
  setnames(cd_ehh, paste("CVR_PCT.", c("C", "DH", "DR", "DW"), sep = ""),
           toupper(c("iherb", "imoss_dh", "imoss_dr", "imoss_dw")))

  c14hdr2_1 <- c14hdr2[,.(CLSTR_ID, PLOT, RADIUS, LYR_CD, LYR = CVR_PCT)]
  cd_eth <- reshape(data = c14hdr2_1,
                    v.names = "LYR",
                    idvar = "CLSTR_ID",
                    timevar = "LYR_CD",
                    direction = "wide",
                    sep = "_")

  c15veg2_1 <- c15veg2[,.(CLSTR_ID, PLOT, ITEM_NO, SPECIES, CVR_PCT, LYR_CD)]
  ## quick check for multiple records for each idvar
  # c15veg2_1[, obslen := length(PLOT), by = c("CLSTR_ID", "ITEM_NO", "SPECIES", "LYR_CD")]
  # c15veg2_1[obslen>1]
  # manually remove CMI6-0143-FO1 Linnbor lay C percentage is 4
  # and corncan percentage is 5
  c15veg2_1 <- c15veg2_1[!(CLSTR_ID == "CMI6-0143-FO1" & ITEM_NO == 26 & LYR_CD == "C" &
              SPECIES == "LINNBOR" & CVR_PCT == 4),]
  c15veg2_1 <- c15veg2_1[!(CLSTR_ID == "CMI6-0143-FO1" & ITEM_NO == 24 & LYR_CD == "C" &
                SPECIES == "CORNCAN" & CVR_PCT == 5),]
  cd_ehi <- reshape(data = c15veg2_1,
                    v.names = "CVR_PCT",
                    timevar = "LYR_CD",
                    idvar = c("CLSTR_ID", "ITEM_NO", "SPECIES"),
                    direction = "wide")
  c14veg2[, LIFE_GRP := NULL]
  # c14veg2[, obslen := length(PLOT), by = c("CLSTR_ID", "ITEM_NO", "SPECIES", "LYR_CD")]
  # c14veg2[obslen>1]
  c14veg2 <- unique(c14veg2)
  cd_eti <- reshape(data = c14veg2,
                    v.names = c("CVR_PCT", "AVG_HT"),
                    timevar = "LYR_CD",
                    idvar = c("CLSTR_ID", "ITEM_NO", "SPECIES"),
                    direction = "wide")
  cd_eti <- cd_eti[,.(CLSTR_ID, PLOT, ITEM_NO, SPECIES,
                      CVR_A = CVR_PCT.A, CVR_B1 = CVR_PCT.B1,
                      CVR_B2 = CVR_PCT.B2, MOSS_DW = CVR_PCT.DW,
                      MOSS_DR = CVR_PCT.DR, MOSS_DH = CVR_PCT.DH,
                      HT_B1 = AVG_HT.B1, HT_B2 = AVG_HT.B2)]
  alldata <- list(cd_ehh = cd_ehh,
                  cd_eth = cd_eth,
                  cd_ehi = cd_ehi,
                  cd_eti = cd_eti)
  dbDisconnect(con)
  if(saveThem){
    saveRDS(alldata, file.path(savePath, "veg_card_vgis.rds"))
  } else if (!saveThem){
    return(alldata)
  } else{
    stop("saveThem must be logical.")
  }
}
