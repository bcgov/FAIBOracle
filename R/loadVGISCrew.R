#' Load the crew data from Oracle database
#'
#'
#' @description This function is to load the crew data from VGIS Oracle database. And
#'              associated them with cluster.
#'
#' @param userName character, Specifies a valid user name in VGIS Oracle database.
#' @param password character, Specifies the password to the user name.
#' @param saveThem logical, Specifies whether the loaded data should be saved or returned.
#'                 The default value is FALSE, which means the function will not save files
#'                 for you.
#' @param savePath character, Specifies the path to save your outputs, you do not need to
#'                 specify if \code{saveThem} is turned off.
#'
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
#' @rdname loadVGISCrew
#' @author Yong Luo
loadVGISCrew <- function(userName, password,
                         saveThem = FALSE, savePath = "."){
  drv <- dbDriver("Oracle")
  connect.string <-"(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)
  (HOST=nrk1-scan.bcgov)(PORT=1521))
  (CONNECT_DATA=(SERVER=DEDICATED)(SERVICE_NAME=ENVPROD1.NRS.BCGOV)))"
  con <- dbConnect(drv, username = userName, password = password,
                   dbname = connect.string)
  c1_mid <- dbGetQuery(con, "SELECT
                       vgis.PLOT_CLUSTERS.PLOT_CLUSTER_SKEY                      AS CL_KEY,
                       vgis.vgis_projects.PROJECT_SKEY                           AS PROJ_KEY,
                       vgis.vgis_projects.PROJECT_BUSINESS_ID                    AS PROJ_ID,
                       vgis.PLOT_CLUSTERS.SAMPLE_EXP_PLOT_NUM                    AS SAMP_NO,
                       vgis.DESIGN_SPECIFICATIONS.BUSINESS_IDENTIFIER_VALUE      AS TYPE_,
                       vgis.MEASMT_VISITATIONS.SAMPLE_INTENT                     AS INTENT,
                       vgis.MEASMT_VISITATIONS.MEASMT_VISITN_NUM                 AS VISIT,
                       vgis.MEASMT_VISITATIONS.MEASMT_VISIT_SKEY                 AS VST_KEY,
                       vgis.MEASMT_VISITATIONS.MEASMT_DATE                       AS MEAS_DT,
                       vgis.PEOPLE.LAST_NAME                                     AS SURNAME,
                       vgis.PEOPLE.FIRST_NAME                                    AS FIRST,
                       vgis.PEOPLE.PER_INITIAL                                   AS INIT,
                       vgis.ROLES.ROLE_CD                                        AS ROLE_CD,
                       vgis.MEASMT_VISITATIONS.PSTAT_PROCESS_STATE_CD            AS STAT_CD,
                       vgis.SAMPLING_CERTIFICATIONS.CERTIFICATION_TYPE           AS CERT_TYP,
                       vgis.QUALITY_ASSURANCE_ACTIVITIES.ACTIVITY_TYPE           AS ACT_TYP,
                       vgis.QUALITY_ASSURANCE_ACTIVITIES.DATE_PERFORMED          AS PERF_DT
                       FROM
                       vgis.PEOPLE ,
                       vgis.ROLES ,
                       vgis.ROLE_ASSOCIATIONS ,
                       vgis.PLOT_CLUSTERS ,
                       vgis.vgis_projects,
                       vgis.PROJECT_ASSOCIATIONS,
                       vgis.MEASMT_VISITATIONS,
                       vgis.QUALITY_ASSURANCE_ACTIVITIES ,
                       vgis.SAMPLING_CERTIFICATIONS,
                       vgis.DESIGN_SPECIFICATIONS
                       WHERE
                       (vgis.ROLE_ASSOCIATIONS.ROLE_ROLE_SKEY  = vgis.ROLES.ROLE_SKEY)
                       AND (vgis.ROLE_ASSOCIATIONS.PER_PERSON_SKEY = vgis.PEOPLE.PERSON_SKEY)
                       AND (vgis.SAMPLING_CERTIFICATIONS.PER_PERSON_SKEY = vgis.PEOPLE.PERSON_SKEY)
                       AND (vgis.MEASMT_VISITATIONS.MEASMT_VISIT_SKEY
                       = vgis.ROLE_ASSOCIATIONS.MEAVT_MEASMT_VISIT_SKEY)
                       AND (vgis.PLOT_CLUSTERS.PLOT_CLUSTER_SKEY
                       = vgis.ROLE_ASSOCIATIONS.PCLTR_PLOT_CLUSTER_SKEY)
                       AND (vgis.PROJECT_ASSOCIATIONS.PRJ_PROJECT_SKEY
                       = vgis.vgis_projects.PROJECT_SKEY)
                       AND (vgis.PROJECT_ASSOCIATIONS.PCLTR_PLOT_CLUSTER_SKEY
                       = vgis.PLOT_CLUSTERS.PLOT_CLUSTER_SKEY)
                       AND (vgis.QUALITY_ASSURANCE_ACTIVITIES.QA_ACTIVITY_SKEY
                       = vgis.ROLE_ASSOCIATIONS.QAA_QA_ACTIVITY_SKEY)
                       AND (vgis.DESIGN_SPECIFICATIONS.DESIGN_SKEY
                       = vgis.PLOT_CLUSTERS.DES_DESIGN_SKEY)") %>%
    data.table
  set(c1_mid, , c("CL_KEY", "PROJ_KEY", "VST_KEY",
                  "MEAS_DT", "ROLE_CD", "STAT_CD"), NULL)
  c1_mid[, ':='(CLSTR_ID = getClusterID(projID = PROJ_ID,
                                        sampleNO = SAMP_NO,
                                        sampleType = TYPE_,
                                        intent = INTENT,
                                        visit = VISIT),
                ACT_TYPE = toupper(ACT_TYP),
                PERF_DT = as.Date(PERF_DT))]
  c1_mid[, CERT_TYPE := paste(sort(unique(CERT_TYP)), collapse = ", "),
         by = c("SURNAME", "FIRST")]
  set(c1_mid, , c("PROJ_ID", "SAMP_NO", "TYPE_",
                  "INTENT", "VISIT", "CERT_TYP"), NULL)
  c1_mid <- unique(c1_mid[,.(ACT_TYPE, CLSTR_ID, INIT, SURNAME, FIRST, CERT_TYPE)],
                   by = c("ACT_TYPE", "CLSTR_ID"))
  c1_mid <- c1_mid[order(ACT_TYPE, CLSTR_ID),]

  c1_oth <- dbGetQuery(con, "SELECT
                       vgis.PLOT_CLUSTERS.PLOT_CLUSTER_SKEY                  AS CL_KEY,
                       vgis.vgis_projects.PROJECT_SKEY                            AS PROJ_KEY,
                       vgis.vgis_projects.PROJECT_BUSINESS_ID                     AS PROJ_ID,
                       vgis.PLOT_CLUSTERS.SAMPLE_EXP_PLOT_NUM                AS SAMP_NO,
                       vgis.DESIGN_SPECIFICATIONS.BUSINESS_IDENTIFIER_VALUE  AS TYPE_,
                       vgis.MEASMT_VISITATIONS.SAMPLE_INTENT                 AS INTENT,
                       vgis.MEASMT_VISITATIONS.MEASMT_VISITN_NUM             AS VISIT,
                       vgis.MEASMT_VISITATIONS.MEASMT_VISIT_SKEY             AS VST_KEY,
                       vgis.MEASMT_VISITATIONS.MEASMT_DATE                   AS MEAS_DT,
                       vgis.PEOPLE.LAST_NAME                                 AS SURNAME,
                       vgis.PEOPLE.FIRST_NAME                                AS FIRST,
                       vgis.PEOPLE.PER_INITIAL                               AS INIT,
                       vgis.ROLES.ROLE_CD                                    AS ROLE_CD,
                       vgis.MEASMT_VISITATIONS.PSTAT_PROCESS_STATE_CD        AS STAT_CD,
                       vgis.SUBPOPULATIONS.POPLTN_ELMNT_CD                   AS POP_CD
                       FROM
                       vgis.PEOPLE ,
                       vgis.ROLES ,
                       vgis.ROLE_ASSOCIATIONS ,
                       vgis.PLOT_CLUSTERS ,
                       vgis.vgis_projects,
                       vgis.PROJECT_ASSOCIATIONS,
                       vgis.MEASMT_VISITATIONS,
                       vgis.DESIGN_SPECIFICATIONS ,
                       vgis.POINT_LOCATION_VISITATIONS ,
                       vgis.CLUSTER_ELEMENTS  ,
                       vgis.BIOMETRIC_DESIGN_SPECIFICATION ,
                       vgis.SUBPOPULATIONS
                       WHERE
                       (vgis.ROLE_ASSOCIATIONS.ROLE_ROLE_SKEY = vgis.ROLES.ROLE_SKEY)
                       AND (vgis.ROLE_ASSOCIATIONS.PER_PERSON_SKEY = vgis.PEOPLE.PERSON_SKEY)
                       AND (vgis.MEASMT_VISITATIONS.MEASMT_VISIT_SKEY
                       = vgis.ROLE_ASSOCIATIONS.MEAVT_MEASMT_VISIT_SKEY)
                       AND (vgis.PLOT_CLUSTERS.PLOT_CLUSTER_SKEY
                       = vgis.ROLE_ASSOCIATIONS.PCLTR_PLOT_CLUSTER_SKEY)
                       AND (vgis.PROJECT_ASSOCIATIONS.PRJ_PROJECT_SKEY
                       = vgis.vgis_projects.PROJECT_SKEY)
                       AND (vgis.PROJECT_ASSOCIATIONS.PCLTR_PLOT_CLUSTER_SKEY
                       = vgis.PLOT_CLUSTERS.PLOT_CLUSTER_SKEY)
                       AND (vgis.DESIGN_SPECIFICATIONS.DESIGN_SKEY
                       = vgis.PLOT_CLUSTERS.DES_DESIGN_SKEY)
                       AND (vgis.ROLE_ASSOCIATIONS.CEV_CE_VISIT_SKEY
                       = vgis.POINT_LOCATION_VISITATIONS.ce_visit_skey )
                       AND (vgis.POINT_LOCATION_VISITATIONS.CELMT_CLUSTER_ELEMENT_SKEY
                       = vgis.CLUSTER_ELEMENTS.cluster_element_skey )
                       AND (vgis.CLUSTER_ELEMENTS.BDSPEC_BD_SPEC_SKEY
                       = vgis.BIOMETRIC_DESIGN_SPECIFICATION.BD_SPEC_SKEY )
                       AND (vgis.BIOMETRIC_DESIGN_SPECIFICATION.subp_sub_pop_skey
                       = vgis.SUBPOPULATIONS.SUB_POP_SKEY )") %>%
    data.table
  set(c1_oth, , c("CL_KEY", "PROJ_KEY", "VST_KEY", "MEAS_DT", "STAT_CD"), NULL)
  c1_oth[,':='(CLSTR_ID = getClusterID(PROJ_ID, SAMP_NO, TYPE_,
                                       INTENT, VISIT))]
  set(c1_oth, , c("PROJ_ID", "SAMP_NO", "TYPE_", "INTENT",
                  "VISIT", "ROLE_CD"), NULL)
  c1_oth <- c1_oth[order(POP_CD, CLSTR_ID, SURNAME, FIRST),
                   .(POP_CD, CLSTR_ID, INIT, SURNAME, FIRST)]

  c1_top <- dbGetQuery(con, "SELECT
                       vgis.PLOT_CLUSTERS.PLOT_CLUSTER_SKEY                              AS CL_KEY,
                       vgis.vgis_projects.PROJECT_SKEY                                        AS PROJ_KEY,
                       vgis.vgis_projects.PROJECT_BUSINESS_ID                                 AS PROJ_ID,
                       vgis.PLOT_CLUSTERS.SAMPLE_EXP_PLOT_NUM                            AS SAMP_NO,
                       vgis.DESIGN_SPECIFICATIONS.BUSINESS_IDENTIFIER_VALUE              AS TYPE_,
                       vgis.MEASMT_VISITATIONS.SAMPLE_INTENT                             AS INTENT ,
                       vgis.MEASMT_VISITATIONS.MEASMT_VISITN_NUM                         AS VISIT,
                       vgis.MEASMT_VISITATIONS.MEASMT_VISIT_SKEY                         AS VST_KEY,
                       vgis.MEASMT_VISITATIONS.MEASMT_DATE                               AS MEAS_DT,
                       vgis.PEOPLE.LAST_NAME                                             AS SURNAME,
                       vgis.PEOPLE.FIRST_NAME                                            AS FIRST,
                       vgis.PEOPLE.PER_INITIAL                                           AS INIT,
                       vgis.ROLES.ROLE_CD                                                AS ROLE_CD,
                       vgis.MEASMT_VISITATIONS.PSTAT_PROCESS_STATE_CD                    AS STAT_CD,
                       NULL
                       FROM
                       vgis.PEOPLE ,
                       vgis.ROLES ,
                       vgis.ROLE_ASSOCIATIONS ,
                       vgis.PLOT_CLUSTERS ,
                       vgis.vgis_projects,
                       vgis.PROJECT_ASSOCIATIONS,
                       vgis.MEASMT_VISITATIONS,
                       vgis.DESIGN_SPECIFICATIONS
                       WHERE
                       (vgis.ROLE_ASSOCIATIONS.ROLE_ROLE_SKEY = vgis.ROLES.ROLE_SKEY)
                       AND (vgis.ROLE_ASSOCIATIONS.PER_PERSON_SKEY = vgis.PEOPLE.PERSON_SKEY)
                       AND (vgis.MEASMT_VISITATIONS.MEASMT_VISIT_SKEY
                       = vgis.ROLE_ASSOCIATIONS.MEAVT_MEASMT_VISIT_SKEY)
                       AND (vgis.PLOT_CLUSTERS.PLOT_CLUSTER_SKEY
                       = vgis.ROLE_ASSOCIATIONS.PCLTR_PLOT_CLUSTER_SKEY)
                       AND (vgis.PROJECT_ASSOCIATIONS.PRJ_PROJECT_SKEY
                       = vgis.vgis_projects.PROJECT_SKEY)
                       AND (vgis.PROJECT_ASSOCIATIONS.PCLTR_PLOT_CLUSTER_SKEY
                       = vgis.PLOT_CLUSTERS.PLOT_CLUSTER_SKEY)
                       AND (vgis.DESIGN_SPECIFICATIONS.DESIGN_SKEY
                       = vgis.PLOT_CLUSTERS.DES_DESIGN_SKEY)
                       AND vgis.ROLE_ASSOCIATIONS.CEV_CE_VISIT_SKEY IS NULL
                       AND vgis.ROLE_ASSOCIATIONS.QAA_QA_ACTIVITY_SKEY IS NULL") %>%
    data.table
  dbDisconnect(con)
  set(c1_top, , c("CL_KEY", "PROJ_KEY", "VST_KEY", "MEAS_DT",
                  "STAT_CD",  "NULL"), NULL)
  c1_top[,':='(CLSTR_ID = getClusterID(PROJ_ID, SAMP_NO, TYPE_,
                                       INTENT, VISIT))]
  set(c1_top, , c("PROJ_ID", "SAMP_NO", "TYPE_", "INTENT", "VISIT"), NULL)
  c1_top <- c1_top[ROLE_CD == "DATA COLLR",.(CLSTR_ID, INIT, SURNAME,
                                             FIRST)]
  c1_top <- c1_top[order(CLSTR_ID, SURNAME, FIRST)]
  if(saveThem){
    saveRDS(list(QAcrews = c1_mid,
                 othcrews = c1_oth,
                 fieldcrews = c1_top),
            file.path(savePath, "crew_card_vgis.rds"))
  } else if (!saveThem){
    return(list(QAcrews = c1_mid,
                othcrews = c1_oth,
                fieldcrews = c1_top))
  } else {
    stop("saveThem must be logical.")
  }
}
