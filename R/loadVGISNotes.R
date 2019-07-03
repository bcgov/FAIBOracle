#' Load the sql note data from Oracle database
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
#' @rdname loadVGISNotes
#' @author Yong Luo
loadVGISNotes <- function(userName, password, saveThem = FALSE,
                          savePath = file.path(".")){
  drv <- dbDriver("Oracle")
  connect.string <-"(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)
  (HOST=nrk1-scan.bcgov)(PORT=1521))
  (CONNECT_DATA=(SERVER=DEDICATED)(SERVICE_NAME=ENVPROD1.NRS.BCGOV)))"
  con <- dbConnect(drv, username = userName, password = password,
                   dbname = connect.string)
  notes <- dbGetQuery(con, "SELECT
                      vgis.PLOT_CLUSTERS.PLOT_CLUSTER_SKEY AS CL_KEY,
                      vgis.vgis_projects.PROJECT_SKEY AS PROJ_KEY,
                      vgis.MEASMT_VISITATIONS.MEASMT_VISIT_SKEY as mv_key,
                      vgis.vgis_projects.PROJECT_BUSINESS_ID AS PROJ_ID,
                      vgis.PLOT_CLUSTERS.SAMPLE_EXP_PLOT_NUM AS SAMP_NO,
                      vgis.DESIGN_SPECIFICATIONS.BUSINESS_IDENTIFIER_VALUE AS TYPE_,
                      vgis.MEASMT_VISITATIONS.SAMPLE_INTENT  AS INTENT ,
                      vgis.MEASMT_VISITATIONS.MEASMT_VISITN_NUM AS VISIT,
                      vgis.ACCESS_NOTES.CMT  AS NOTES,
                      vgis.access_notes.sequence_id as seq_no
                      FROM
                      vgis.PLOT_CLUSTERS ,
                      vgis.vgis_projects,
                      vgis.PROJECT_ASSOCIATIONS,
                      vgis.MEASMT_VISITATIONS,
                      vgis.DESIGN_SPECIFICATIONS,
                      vgis.POINT_LOCATIONS,
                      vgis.ACCESS_NOTES
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
                      AND (vgis.ACCESS_NOTES.PLOC_POINT_LOCN_SKEY
                      = vgis.POINT_LOCATIONS.POINT_LOCN_SKEY)") %>%
    data.table
  set(notes, , c("CL_KEY", "PROJ_KEY", "MV_KEY"), NULL)
  notes[,':='(CLSTR_ID = getClusterID(PROJ_ID, SAMP_NO, TYPE_,
                                      INTENT, VISIT))]
  set(notes, , c("PROJ_ID", "SAMP_NO", "TYPE_", "INTENT", "VISIT"), NULL)
  note_all <- dbGetQuery(con, "SELECT
                         vgis.PLOT_CLUSTERS.PLOT_CLUSTER_SKEY AS CL_KEY,
                         vgis.vgis_projects.PROJECT_SKEY AS PROJ_KEY,
                         vgis.MEASMT_VISITATIONS.MEASMT_VISIT_SKEY as mv_key,
                         vgis.vgis_projects.PROJECT_BUSINESS_ID AS PROJ_ID,
                         vgis.PLOT_CLUSTERS.SAMPLE_EXP_PLOT_NUM AS SAMP_NO,
                         vgis.DESIGN_SPECIFICATIONS.BUSINESS_IDENTIFIER_VALUE AS TYPE_,
                         vgis.MEASMT_VISITATIONS.SAMPLE_INTENT  AS INTENT ,
                         vgis.MEASMT_VISITATIONS.MEASMT_VISITN_NUM AS VISIT,
                         vgis.VISITATION_NOTES.NOTE_TEXT  AS NOTES,
                         vgis.VISITATION_notes.seq_NUM   as seq_no,
                         vgis.VISITATION_notes.NOTE_TYPE as NOTE_TYP

                         FROM
                         vgis.PLOT_CLUSTERS ,
                         vgis.vgis_projects,
                         vgis.PROJECT_ASSOCIATIONS,
                         vgis.MEASMT_VISITATIONS,
                         vgis.DESIGN_SPECIFICATIONS,
                         vgis.VISITATION_NOTES
                         WHERE
                         (vgis.PROJECT_ASSOCIATIONS.PRJ_PROJECT_SKEY
                         = vgis.vgis_projects.PROJECT_SKEY)
                         AND (vgis.PROJECT_ASSOCIATIONS.PCLTR_PLOT_CLUSTER_SKEY
                         = vgis.PLOT_CLUSTERS.PLOT_CLUSTER_SKEY)
                         AND (vgis.DESIGN_SPECIFICATIONS.DESIGN_SKEY
                         = vgis.PLOT_CLUSTERS.DES_DESIGN_SKEY)
                         AND (vgis.MEASMT_VISITATIONS.PCLTR_PLOT_CLUSTER_SKEY
                         = vgis.PLOT_CLUSTERS.PLOT_CLUSTER_SKEY)
                         AND (vgis.VISITATION_NOTES.PCLTR_PLOT_CLUSTER_SKEY
                         = vgis.PLOT_CLUSTERS.PLOT_CLUSTER_SKEY)") %>%
    data.table
  set(note_all, , c("CL_KEY", "PROJ_KEY", "MV_KEY"), NULL)
  note_all[,':='(CLSTR_ID = getClusterID(PROJ_ID, SAMP_NO, TYPE_,
                                         INTENT, VISIT))]
  set(note_all, , c("PROJ_ID", "SAMP_NO", "TYPE_", "INTENT", "VISIT"), NULL)
  notes <- list(accessNotes = notes[order(CLSTR_ID, SEQ_NO),
                                    .(CLSTR_ID, SEQ_NO, NOTES)],
                allNotes = note_all[order(CLSTR_ID, SEQ_NO),
                                    .(CLSTR_ID, SEQ_NO, NOTE_TYP, NOTES)])
  dbDisconnect(con)
  if(saveThem){
    saveRDS(notes, file.path(savePath, "notes_card_vgis.rds"))
  } else if (!saveThem){
    return(notes)
  } else{
    stop("saveThem must be logical.")
  }
}
