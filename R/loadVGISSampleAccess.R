#' Load the sample access data from Oracle database
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
#' @rdname loadVGISSampleAccess
#' @author Yong Luo
loadVGISSampleAccess <- function(userName, passWord, saveThem = FALSE,
                                 savePath = file.path(".")){
  drv <- dbDriver("Oracle")
  connect_string <- getServer(databaseName = "VGIS")
  con <- dbConnect(drv, username = userName, password = passWord,
                   dbname = connect_string)
  cd_cp1 <- dbGetQuery(con, "SELECT
                       vgis.PLOT_CLUSTERS.PLOT_CLUSTER_SKEY as cl_key,
                       vgis.vgis_projects.PROJECT_SKEY as proj_key,
                       vgis.MEASMT_VISITATIONS.MEASMT_VISIT_SKEY as vst_key,
                       vgis.vgis_projects.PROJECT_BUSINESS_ID as proj_id,                 /*-- Project  */
                       vgis.PLOT_CLUSTERS.SAMPLE_EXP_PLOT_NUM AS SAMP_NO,    /*-- Plot Sample No */
                       vgis.DESIGN_SPECIFICATIONS.BUSINESS_IDENTIFIER_VALUE as type_,        /*-- Sample Type */
                       vgis.MEASMT_VISITATIONS.SAMPLE_INTENT as intent,              /*-- Sample Intent  */
                       vgis.MEASMT_VISITATIONS.MEASMT_VISITN_NUM as visit,           /*-- Sample No  */
                       vgis.POINT_LOCATIONS.LOCATION_TYPE_CD as loc_cd,              /*-- Point Location Type  */
                       vgis.POINT_LOCATIONS.DESCRIPTION as ap_dsc,                   /*-- Point Description, Access Point only  */
                       vgis.POINT_LOCATIONS.LOCATION_FILE_ID as ap_gpsid,            /*-- GPS File ID, Access, Reference and IPC */
                       vgis.POINT_LOCATIONS.UTM_ZONE as ap_utm,                      /*-- GPS UTM Zone:Access and IPC  */
                       vgis.POINT_LOCATIONS.UTM_EASTING as ap_east,                  /*-- GPS Northing:Access, Reference and IPC */
                       vgis.POINT_LOCATIONS.UTM_NORTHING as ap_nrth,                 /*-- GPS Easting:Access, Reference and IPC */
                       vgis.POINT_LOCATIONS.ELEVATION as ap_elev                     /*-- Point Elevation: Access, Reference and IPC */
                       FROM
                       vgis.PLOT_CLUSTERS ,
                       vgis.vgis_projects,
                       vgis.PROJECT_ASSOCIATIONS,
                       vgis.MEASMT_VISITATIONS,
                       vgis.DESIGN_SPECIFICATIONS,
                       vgis.POINT_LOCATIONS
                       WHERE
                       (vgis.PROJECT_ASSOCIATIONS.PRJ_PROJECT_SKEY
                       = vgis.vgis_projects.PROJECT_SKEY)
                       AND (vgis.PROJECT_ASSOCIATIONS.PCLTR_PLOT_CLUSTER_SKEY
                       = vgis.PLOT_CLUSTERS.PLOT_CLUSTER_SKEY)
                       AND (vgis.DESIGN_SPECIFICATIONS.DESIGN_SKEY
                       = vgis.PLOT_CLUSTERS.DES_DESIGN_SKEY)
                       AND (vgis.MEASMT_VISITATIONS.PCLTR_PLOT_CLUSTER_SKEY
                       = vgis.PLOT_CLUSTERS.PLOT_CLUSTER_SKEY)
                       AND (vgis.POINT_LOCATIONS.PCLTR_PLOT_CLUSTER_SKEY
                       = vgis.PLOT_CLUSTERS.PLOT_CLUSTER_SKEY)
                       AND (vgis.POINT_LOCATIONS.LOCATION_TYPE_CD = 'ACCESS POINT')") %>%
    data.table
  set(cd_cp1, , c("CL_KEY", "PROJ_KEY", "VST_KEY"), NULL)
  cd_cp1[, CLSTR_ID := getClusterID(PROJ_ID, SAMP_NO, TYPE_,
                                    INTENT, VISIT)]
  set(cd_cp1, , c("PROJ_ID", "SAMP_NO", "TYPE_",
                  "INTENT", "VISIT"), NULL)
  cd_cp1 <- unique(cd_cp1[,.(CLSTR_ID, LOC_CD, AP_DSC, AP_GPSID,
                             AP_UTM, AP_EAST, AP_NRTH, AP_ELEV)],
                   by = "CLSTR_ID")

  cd_cp2 <- dbGetQuery(con, "SELECT
                       POINT_LOCN_SKEY,
                       vgis.PLOT_CLUSTERS.PLOT_CLUSTER_SKEY AS CL_KEY,
                       vgis.vgis_projects.PROJECT_SKEY AS PROJ_KEY,
                       vgis.MEASMT_VISITATIONS.MEASMT_VISIT_SKEY AS VST_KEY,
                       vgis.vgis_projects.PROJECT_BUSINESS_ID AS PROJ_ID,                 /*-- Project  */
                       vgis.PLOT_CLUSTERS.SAMPLE_EXP_PLOT_NUM AS SAMP_NO,    /*-- Plot Sample No  */
                       vgis.DESIGN_SPECIFICATIONS.BUSINESS_IDENTIFIER_VALUE AS TYPE_,        /*-- Sample Type  */
                       vgis.MEASMT_VISITATIONS.SAMPLE_INTENT AS INTENT ,             /*-- Sample Intent  */
                       vgis.MEASMT_VISITATIONS.MEASMT_VISITN_NUM AS VISIT,           /*-- Sample No  */
                       vgis.POINT_LOCATIONS.LOCATION_TYPE_CD AS LOC_CD,              /*-- Point Location Type  */
                       vgis.POINT_LOCATIONS.REFERENCE_TREE_BRG_TO_POINT AS TP_AZ_tr, /*-- Reference Bearing to Point, Tie Point or Reference Point*/
                       vgis.POINT_LOCATIONS.REF_TREE_HOR_DIST_TO_POINT AS TP_DT_tr,  /*-- Reference Bearing to Point, Tie Point or Reference Point*/
                       vgis.POINT_LOCATIONS.LOCATION_FILE_ID AS tp_gpsid,            /*-- GPS File ID, Access, Reference and IPC*/
                       vgis.POINT_LOCATIONS.UTM_ZONE AS tp_UTM,                      /*-- GPS UTM Zone:Access and IPC*/
                       vgis.POINT_LOCATIONS.UTM_EASTING AS tp_east,                  /*-- GPS Northing:Access, Reference and IPC*/
                       vgis.POINT_LOCATIONS.UTM_NORTHING AS tp_nrTH,                 /*-- GPS Easting:Access, Reference and IPC*/
                       vgis.POINT_LOCATIONS.UTM_OFFSET_BEARING AS TP_AZ_GP,          /*-- GPS Offset Bearing*/
                       vgis.POINT_LOCATIONS.UTM_OFFSET_DISTANCE AS TP_DT_GP,         /*-- GPS Offset Distance*/
                       vgis.POINT_LOCATIONS.ELEVATION AS tp_ELEV,                    /*-- Point Elevation: Access, Reference and IPC */
                       vgis.POINT_LOCATIONS.REF_TYPE AS REF_TYPE,                    /*-- Reference Type: TREE, ROCK or OTHER  */
                       vgis.POINT_LOCATIONS.REF_NUM AS TIE_TRNO,                             /*-- Reference Number i.e. Tree# */
                       vgis.POINT_LOCATIONS.AERIAL_FLIGHT_LINE_ID AS tp_fltln,               /*-- Flight Line */
                       vgis.POINT_LOCATIONS.AERIAL_PHOTO_NUM AS tp_photo,            /*-- Photo Num  */
                       vgis.POINT_LOCATIONS.POLYGON_NUMBER AS tp_poly,                       /*-- Temp Polygon no*/
                       m3.COMPONENT_VALUE ||
                       m2.COMPONENT_VALUE ||
                       m1.COMPONENT_VALUE  AS tp_MAp
                       FROM
                       vgis.PLOT_CLUSTERS ,
                       vgis.vgis_projects,
                       vgis.PROJECT_ASSOCIATIONS,
                       vgis.MEASMT_VISITATIONS,
                       vgis.DESIGN_SPECIFICATIONS,
                       vgis.POINT_LOCATIONS,
                       MAPSHEET_COMPONENTS m1,
                       MAPSHEET_COMPONENTS m2,
                       MAPSHEET_COMPONENTS m3
                       WHERE
                       (vgis.PROJECT_ASSOCIATIONS.PRJ_PROJECT_SKEY
                       = vgis.vgis_projects.PROJECT_SKEY)
                       AND (vgis.PROJECT_ASSOCIATIONS.PCLTR_PLOT_CLUSTER_SKEY
                       = vgis.PLOT_CLUSTERS.PLOT_CLUSTER_SKEY)
                       AND (vgis.DESIGN_SPECIFICATIONS.DESIGN_SKEY
                       = vgis.PLOT_CLUSTERS.DES_DESIGN_SKEY)
                       AND (vgis.MEASMT_VISITATIONS.PCLTR_PLOT_CLUSTER_SKEY
                       = vgis.PLOT_CLUSTERS.PLOT_CLUSTER_SKEY)
                       AND (vgis.POINT_LOCATIONS.PCLTR_PLOT_CLUSTER_SKEY
                       = vgis.PLOT_CLUSTERS.PLOT_CLUSTER_SKEY)
                       AND (vgis.POINT_LOCATIONS.LOCATION_TYPE_CD = 'TIE POINT')
                       AND m1.MCMP_MAP_COMP_SKEY = m2.MAP_COMP_SKEY(+)
                       AND m2.MCMP_MAP_COMP_SKEY = m3.MAP_COMP_SKEY(+)
                       AND m1.MAP_COMP_SKEY(+) = vgis.point_locations.MCMP_MAP_COMP_SKEY") %>%
    data.table
  set(cd_cp2, , c("POINT_LOCN_SKEY", "CL_KEY", "PROJ_KEY", "VST_KEY"), NULL)
  cd_cp2[, CLSTR_ID := getClusterID(PROJ_ID, SAMP_NO, TYPE_,
                                    INTENT, VISIT)]
  set(cd_cp2, , c("PROJ_ID", "SAMP_NO", "TYPE_",
                  "INTENT", "VISIT"), NULL)



  cd_cp3 <- dbGetQuery(con, "SELECT
                       vgis.PLOT_CLUSTERS.PLOT_CLUSTER_SKEY as cl_key,
                       vgis.vgis_projects.PROJECT_SKEY as proj_key,
                       vgis.MEASMT_VISITATIONS.MEASMT_VISIT_SKEY as vst_key,
                       vgis.vgis_projects.PROJECT_BUSINESS_ID as proj_id,                    /*-- Project */
                       vgis.PLOT_CLUSTERS.SAMPLE_EXP_PLOT_NUM AS SAMP_NO,               /*-- Plot Sample No */
                       vgis.DESIGN_SPECIFICATIONS.BUSINESS_IDENTIFIER_VALUE as type_,   /*-- Sample Type */
                       vgis.MEASMT_VISITATIONS.SAMPLE_INTENT  as intent,             /*-- Sample Intent */
                       vgis.MEASMT_VISITATIONS.MEASMT_VISITN_NUM as visit,
                       vgis.POINT_LOCATIONS.LOCATION_TYPE_CD as loc_cd,              /*-- Point Location Type */
                       vgis.POINT_LOCATIONS.ORIGIN_DIST as tP_DT_iP,                 /*-- Distance to IPC */
                       vgis.POINT_LOCATIONS.ORIGIN_BRG tp_AZ_iP,                     /*-- Bearing to IPC */
                       vgis.POINT_LOCATIONS.MAGNETIC_DECLINATION as declin,          /*-- Declination to IPC  */
                       vgis.POINT_LOCATIONS.OFFSET_BEARING_TO_POINT as rp_az_pn,     /*-- Reference Pin Offset Bearing */
                       vgis.POINT_LOCATIONS.OFFSET_HOR_DIST_TO_POINT as rp_dt_pn,    /*-- Reference Pin Offset Distance */
                       vgis.POINT_LOCATIONS.REFERENCE_TREE_BRG_TO_POINT as rp_az_tr, /*-- Reference Tree Bearing to Point */
                       vgis.POINT_LOCATIONS.REF_TREE_HOR_DIST_TO_POINT as rp_dt_tr,  /*-- Reference Bearing to Point */
                       vgis.POINT_LOCATIONS.LOCATION_FILE_ID as rp_gpsid,            /*-- GPS File ID */
                       vgis.POINT_LOCATIONS.UTM_ZONE as rp_utm,                      /*-- GPS UTM Zone */
                       vgis.POINT_LOCATIONS.UTM_EASTING as rp_nrth,                  /*-- GPS Northing */
                       vgis.POINT_LOCATIONS.UTM_NORTHING as rp_east,                 /*-- GPS Easting */
                       vgis.POINT_LOCATIONS.ELEVATION as rp_elev,                    /*-- Point Elevation  */
                       vgis.POINT_LOCATIONS.REF_TYPE as rp_type,                     /*-- Reference Type: TREE, ROCK or OTHER */
                       vgis.POINT_LOCATIONS.REF_NUM as rp_num                        /*-- Reference Number i.e. Tree# */
                       FROM
                       vgis.PLOT_CLUSTERS ,
                       vgis.vgis_projects,
                       vgis.PROJECT_ASSOCIATIONS,
                       vgis.MEASMT_VISITATIONS,
                       vgis.DESIGN_SPECIFICATIONS,
                       vgis.POINT_LOCATIONS
                       WHERE
                       (vgis.PROJECT_ASSOCIATIONS.PRJ_PROJECT_SKEY
                       = vgis.vgis_projects.PROJECT_SKEY)
                       AND (vgis.PROJECT_ASSOCIATIONS.PCLTR_PLOT_CLUSTER_SKEY
                       = vgis.PLOT_CLUSTERS.PLOT_CLUSTER_SKEY)
                       AND (vgis.DESIGN_SPECIFICATIONS.DESIGN_SKEY
                       = vgis.PLOT_CLUSTERS.DES_DESIGN_SKEY)
                       AND (vgis.MEASMT_VISITATIONS.PCLTR_PLOT_CLUSTER_SKEY
                       = vgis.PLOT_CLUSTERS.PLOT_CLUSTER_SKEY)
                       AND (vgis.POINT_LOCATIONS.PCLTR_PLOT_CLUSTER_SKEY
                       = vgis.PLOT_CLUSTERS.PLOT_CLUSTER_SKEY)
                       AND (vgis.POINT_LOCATIONS.LOCATION_TYPE_CD ='REFERENCE POINT' )") %>%
    data.table

  set(cd_cp3, , c("CL_KEY", "PROJ_KEY", "VST_KEY"), NULL)
  cd_cp3[, CLSTR_ID := getClusterID(PROJ_ID, SAMP_NO, TYPE_,
                                    INTENT, VISIT)]
  set(cd_cp3, , c("PROJ_ID", "SAMP_NO", "TYPE_",
                  "INTENT", "VISIT"), NULL)


  c2_d <- dbGetQuery(con, "SELECT
                     POINT_LOCN_SKEY,
                     vgis.PLOT_CLUSTERS.PLOT_CLUSTER_SKEY as cl_key,
                     vgis.vgis_projects.PROJECT_SKEY as proj_key,
                     vgis.MEASMT_VISITATIONS.MEASMT_VISIT_SKEY as vst_key,
                     vgis.vgis_projects.PROJECT_BUSINESS_ID as proj_id,                 /*-- Project  */
                     vgis.PLOT_CLUSTERS.SAMPLE_EXP_PLOT_NUM AS SAMP_NO,            /*-- Plot Sample No  */
                     vgis.DESIGN_SPECIFICATIONS.BUSINESS_IDENTIFIER_VALUE as type_,  /*-- Sample Type */
                     vgis.MEASMT_VISITATIONS.SAMPLE_INTENT  as intent,             /*-- Sample Intent */
                     vgis.MEASMT_VISITATIONS.MEASMT_VISITN_NUM as visit,           /*-- Sample No */
                     vgis.POINT_LOCATIONS.LOCATION_TYPE_CD as loc_cd,              /*-- Point Location Type  */
                     vgis.POINT_LOCATIONS.OFFSET_BEARING_TO_POINT AS ip_az_pn,     /*-- IPC Pin Offset Bearing */
                     vgis.POINT_LOCATIONS.OFFSET_HOR_DIST_TO_POINT AS IP_dt_pn,    /*-- IPC Pin Offset Distance */
                     vgis.POINT_LOCATIONS.LOCATION_FILE_ID AS IP_gpsid,            /*-- GPS File ID */
                     vgis.POINT_LOCATIONS.UTM_ZONE AS IP_UTM,                      /*-- GPS UTM Zone */
                     vgis.POINT_LOCATIONS.UTM_EASTING AS IP_EAST,                  /*-- GPS Northing */
                     vgis.POINT_LOCATIONS.UTM_NORTHING AS IP_NRTH,                 /*-- GPS Easting */
                     vgis.POINT_LOCATIONS.UTM_OFFSET_BEARING AS IP_az_gp,          /*-- GPS Offset Bearing  */
                     vgis.POINT_LOCATIONS.UTM_OFFSET_DISTANCE AS IP_dt_gp,         /*-- GPS Offset Distance  */
                     vgis.POINT_LOCATIONS.ELEVATION as IP_elev,                    /*-- Point Elevation  */
                     vgis.POINT_LOCATIONS.AERIAL_FLIGHT_LINE_ID as ip_fltln,       /*-- Flight Line  */
                     vgis.POINT_LOCATIONS.AERIAL_PHOTO_NUM as ip_photo,            /*-- Photo Num */
                     vgis.POINT_LOCATIONS.POLYGON_NUMBER as ip_poly,               /*-- Temp Polygon No  */
                     m3.COMPONENT_VALUE ||
                     m2.COMPONENT_VALUE ||
                     m1.COMPONENT_VALUE                 as ip_map
                     FROM
                     vgis.PLOT_CLUSTERS ,
                     vgis.vgis_projects,
                     vgis.PROJECT_ASSOCIATIONS,
                     vgis.MEASMT_VISITATIONS,
                     vgis.DESIGN_SPECIFICATIONS,
                     vgis.POINT_LOCATIONS,
                     vgis.MAPSHEET_COMPONENTS m1,
                     vgis.MAPSHEET_COMPONENTS m2,
                     vgis.MAPSHEET_COMPONENTS m3
                     WHERE
                     (vgis.PROJECT_ASSOCIATIONS.PRJ_PROJECT_SKEY
                     = vgis.vgis_projects.PROJECT_SKEY)
                     AND (vgis.PROJECT_ASSOCIATIONS.PCLTR_PLOT_CLUSTER_SKEY
                     = vgis.PLOT_CLUSTERS.PLOT_CLUSTER_SKEY)
                     AND (vgis.DESIGN_SPECIFICATIONS.DESIGN_SKEY
                     = vgis.PLOT_CLUSTERS.DES_DESIGN_SKEY)
                     AND (vgis.MEASMT_VISITATIONS.PCLTR_PLOT_CLUSTER_SKEY
                     = vgis.PLOT_CLUSTERS.PLOT_CLUSTER_SKEY)
                     AND (vgis.POINT_LOCATIONS.PCLTR_PLOT_CLUSTER_SKEY
                     = vgis.PLOT_CLUSTERS.PLOT_CLUSTER_SKEY)
                     AND (vgis.POINT_LOCATIONS.POINT_TYPE = 'PLOT CTR')
                     AND m1.MCMP_MAP_COMP_SKEY = m2.MAP_COMP_SKEY(+)
                     AND m2.MCMP_MAP_COMP_SKEY = m3.MAP_COMP_SKEY(+)
                     AND m1.MAP_COMP_SKEY(+) = vgis.point_locations.MCMP_MAP_COMP_SKEY") %>%
    data.table

  set(c2_d, , c("CL_KEY", "PROJ_KEY", "VST_KEY"), NULL)
  c2_d[, CLSTR_ID := getClusterID(PROJ_ID, SAMP_NO, TYPE_,
                                  INTENT, VISIT)]
  set(c2_d, , c("PROJ_ID", "SAMP_NO", "TYPE_",
                "INTENT", "VISIT"), NULL)
  card_cl <- c2_d[,.(CLSTR_ID, IP_AZ_PN, IP_DT_PN, IP_AZ_GP,
                     IP_DT_GP, IP_GPSID, IP_UTM, IP_EAST, IP_NRTH, IP_ELEV)]
  cd_cp4 <- c2_d[,.(CLSTR_ID, IP_MAP, IP_FLTLN, IP_POLY, IP_PHOTO)]
  rm(c2_d)

  cd_cp5 <- dbGetQuery(con, "SELECT
                       vgis.PLOT_CLUSTERS.PLOT_CLUSTER_SKEY as cl_key,
                       vgis.vgis_projects.PROJECT_SKEY as proj_key,
                       vgis.MEASMT_VISITATIONS.MEASMT_VISIT_SKEY as vst_key,
                       vgis.vgis_projects.PROJECT_BUSINESS_ID as proj_id,                 /*-- Project */
                       vgis.PLOT_CLUSTERS.SAMPLE_EXP_PLOT_NUM AS SAMP_NO,    /*-- Plot Sample No */
                       vgis.DESIGN_SPECIFICATIONS.BUSINESS_IDENTIFIER_VALUE as type_,        /*-- Sample Type */
                       vgis.MEASMT_VISITATIONS.SAMPLE_INTENT as intent ,             /*-- Sample Intent */
                       vgis.MEASMT_VISITATIONS.MEASMT_VISITN_NUM as visit,           /*-- Sample No */
                       vgis.POINT_LOCATIONS.LOCATION_TYPE_CD as loc_cd,              /*-- Point Location Type  */
                       vgis.POINT_LOCATIONS.REFERENCE_TREE_BRG_TO_POINT AS AZIM,     /*-- Ref Bearing to Point, Tie Pt or Ref Point  */
                       vgis.POINT_LOCATIONS.REF_TREE_HOR_DIST_TO_POINT AS DIST,      /*-- Ref Bearing to Point, Tie Pt or Ref Point  */
                       vgis.TREES.REF_TREE_LAST_DIAMETER as dbh,                     /*-- Reference Tree Diameter */
                       vgis.TAXONOMIC_NAMES.LONG_CD as LONG_SPECIES              /*-- Reference Tree LONG_SPECIES   */
                       FROM
                       vgis.PLOT_CLUSTERS ,
                       vgis.vgis_projects,
                       vgis.PROJECT_ASSOCIATIONS,
                       vgis.MEASMT_VISITATIONS,
                       vgis.DESIGN_SPECIFICATIONS,
                       vgis.POINT_LOCATIONS,
                       vgis.TREES,
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
                       AND (vgis.POINT_LOCATIONS.PCLTR_PLOT_CLUSTER_SKEY
                       = vgis.PLOT_CLUSTERS.PLOT_CLUSTER_SKEY)
                       AND (vgis.POINT_LOCATIONS.POINT_TYPE IN ('PLOT REF','OUT PLOT'))
                       AND (vgis.POINT_LOCATIONS.POINT_LOCN_SKEY
                       = vgis.TREES.PLOC_POINT_LOCN_SKEY )
                       AND (vgis.TREES.GTO_GROUP_TAXON_SKEY
                       =vgis.GROUP_TAXON_MEMBERSHIPS.GROUP_TAXON_SKEY)
                       AND (vgis.GROUP_TAXON_MEMBERSHIPS.TAXC_TAXONOMIC_CD_SKEY
                       =vgis.TAXONOMIC_NAMES.TAXONOMIC_CD_SKEY)") %>%
    data.table
  set(cd_cp5, , c("CL_KEY", "PROJ_KEY", "VST_KEY"), NULL)
  cd_cp5[, CLSTR_ID := getClusterID(PROJ_ID, SAMP_NO, TYPE_,
                                    INTENT, VISIT)]
  set(cd_cp5, , c("PROJ_ID", "SAMP_NO", "TYPE_",
                  "INTENT", "VISIT"), NULL)
  cd_cp5 <- unique(cd_cp5, by = c("LOC_CD", "CLSTR_ID"))
  access_card <- list(accessPoint = cd_cp1,
                      tiePoint = cd_cp2,
                      referencePoint = cd_cp3,
                      referenceTree = cd_cp5,
                      IPCMap = cd_cp4,
                      IPCLocation = card_cl)
  dbDisconnect(con)
  if(saveThem){
    saveRDS(access_card, file.path(savePath, "access_card_vgis.rds"))
  } else if (!saveThem){
    return(access_card)
  } else{
    stop("saveThem must be logical.")
  }
}
