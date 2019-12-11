#' Load the data from ISMC Oracle database using site name(s)
#'
#'
#' @description This function is to load the ground sample data from ISMC database using
#'              site name(s).
#'
#' @param userName character, Specifies a valid user name in ISMC Oracle database.
#' @param passWord character, Specifies the password to the user name.
#' @param env character, Specifies which environment the data reside. Currently,
#'                               the function supports \code{INT} (intergration)
#'                               and \code {TEST} (test) environment.
#' @param siteName character, Site name.
#' @param savePath character, Specifies the path to save your outputs. If missing, the current working
#'                 directory will be choosed.
#'
#' @param saveName character, Specifies the save name.
#' @param saveFormat character, Specifies the format for the output data.
#'                   It accepts \code{rds}, \code{csv}, \code{txt}, \code{xlsx} and \code{rdata}.
#'                   Default is \code{rdata}.
#' @param overWrite logical, Determine if the file with same name as user specifies
#'                           will be overwritten. Default is \code{FALSE}.
#'
#' @return No value returned. There are 16 tables will be saved with prefix of \code{saveName}.
#'         These tables are SampleSites, AccessNotes,
#'         SampleSiteVisits, GroundSampleCrewActivities, PlotDetails, SampleMeasurements,
#'         SmallLiveTreeTallies, TreeMeasurements, Trees, TreeDetails,
#'         TreeDamageOccurrences, TreeLossIndicators, TreeLogAssessments,
#'         StumpTallies, SiteNavigation,
#'         IntegratedPlotCenter, ReferencePoint and TiePoint.
#'         Note that if the save format is \code{xlsx}, all the tables will be saved into one workbook.
#'
#' @importFrom data.table ':=' data.table
#' @importFrom dplyr '%>%'
#' @importFrom ROracle dbConnect dbGetQuery dbDisconnect
#' @importFrom DBI dbDriver
#' @importFrom openxlsx createWorkbook addWorksheet writeData saveWorkbook
#' @importFrom utils write.csv write.table
#' @export
#'
#' @rdname loadISMC_bySiteName
#' @author Yong Luo
loadISMC_bySiteName <- function(userName, passWord, env,
                                siteName,
                                savePath = ".", saveName,
                                saveFormat = "rdata",
                                overWrite = FALSE){

  if(overWrite & file.exists(file.path(savePath, paste0(saveName, ".", saveFormat)))){
    file.remove(file.path(savePath, paste0(saveName, ".", saveFormat)))
    warning("The original file has been overwritten.")
  }

  if(!overWrite & file.exists(file.path(savePath, paste0(saveName, ".", saveFormat)))){
    cat("Please use different file name, as it is in use. Or turn on overWrite arguement to overwrite the current file.")
    thisopt <- options(show.error.messages = FALSE)
    on.exit(options(thisopt))
    stop()
  }
  siteName_org <- siteName
  if(length(siteName) == 1){
    siteName <- paste0("('", siteName,"')")
  } else {

    siteName <- paste0("('", paste0(siteName, collapse = "', '"),"')")
  }
  drv <- dbDriver("Oracle")
  connect_to_ismc <- getServer(databaseName = "ISMC",
                                 envir = env)
  con <- dbConnect(drv, username = userName,
                   password = passWord,
                   dbname = connect_to_ismc)

  SampleSites <-
    dbGetQuery(con,
               paste0("select
                      ss.*,
                      plc.utm_zone,
                      plc.utm_northing,
                      plc.utm_easting,
                      plc.elevation,
                      plc.point_location_type_code,
                      plc.coordinate_source_code,
                      pspss.*,
                      rcl.*

                      from
                      app_ismc.sample_site ss

                      left join app_ismc.point_location plc
                      on plc.point_location_guic = ss.point_location_guic

                      left join app_ismc.psp_sample_site pspss
                      on pspss.sample_site_guic = ss.sample_site_guic

                      -- ismc_rcl_mvw is materialized view
                      left join app_ismc.ismc_rcl_mvw rcl
                      on rcl.areal_unit_guic = pspss.areal_unit_guic

                      where
                      ss.sample_site_name in ", siteName,
                      "order by sample_site_name")) %>%
    data.table

  if(nrow(SampleSites) == 0){
    stop(paste0("There is no record for the sample site(s): ",
                paste0(siteName_org, collapse = ", ")))
  }

  norecordsample <- siteName_org[!(siteName_org %in% unique(SampleSites$SAMPLE_SITE_NAME))]
  if(length(norecordsample) != 0){
    warning(paste0("There is no record for the sample site(s): ",
                   paste0(norecordsample, collapse = ", ")))
  }
  rm(siteName_org, norecordsample)
  SampleSites <- cleanColumns(SampleSites, level = "sample_site")
  SampleSites <- unique(SampleSites)

  AccessNotes <-
    dbGetQuery(con,
               paste0("select
                      ss.sample_site_name,
                      an.*

                      from
                      app_ismc.access_note an

                      left join app_ismc.sample_site ss
                      on ss.sample_site_guic = an.sample_site_guic

                      where
                      ss.sample_site_name in ", siteName,
                      "order by sample_site_name, sequence_number")) %>%
    data.table
  AccessNotes <- cleanColumns(AccessNotes, level = "sample_site")
  AccessNotes <- unique(AccessNotes)

  SampleSiteVisits <-
    dbGetQuery(con,
               paste0("select
                      ss.sample_site_name,
                      gsp.*,
                      ssv.*

                      from
                      app_ismc.sample_site_visit ssv

                      left join app_ismc.sample_site ss
                      on ss.sample_site_guic = ssv.sample_site_guic

                      left join app_ismc.ground_sample_project gsp
                      on gsp.ground_sample_project_guic = ssv.ground_sample_project_guic

                      where
                      ss.sample_site_name in ", siteName,
                      "order by sample_site_name, visit_number, project_name,
                      sample_site_purpose_type_code")) %>%
    data.table
  SampleSiteVisits <- cleanColumns(SampleSiteVisits, level = "site_visit")

  GroundSampleCrewActivities <-
    dbGetQuery(con,
               paste0("select
                      ss.sample_site_name,
                      gsp.project_name,
                      gsp.project_number,
                      ssv.sample_site_purpose_type_code,
                      ssv.visit_number,
                      gsca.*,
                      gshr.*,
                      cc.*

                      from
                      app_ismc.ground_sample_crew_actvty gsca

                      left join app_ismc.sample_site_visit ssv
                      on ssv.sample_site_visit_guic = gsca.sample_site_visit_guic

                      left join app_ismc.ground_sample_project gsp
                      on gsp.ground_sample_project_guic = ssv.ground_sample_project_guic

                      left join app_ismc.sample_site ss
                      on ss.sample_site_guic = ssv.sample_site_guic

                      left join app_ismc.ground_sample_human_rsrce gshr
                      on gshr.ground_sample_human_rsrce_guic = gsca.ground_sample_human_rsrce_guic

                      left join app_ismc.crew_certification cc
                      on cc.ground_sample_human_rsrce_guic = gsca.ground_sample_human_rsrce_guic

                      where
                      ss.sample_site_name in ", siteName,
                      "order by
                      sample_site_name, visit_number, project_name")) %>%
    data.table
  GroundSampleCrewActivities <- cleanColumns(GroundSampleCrewActivities, level = "site_visit")

  PlotDetails <-
    dbGetQuery(con,
               paste0("select
                      ss.sample_site_name,
                      gsp.project_name,
                      gsp.project_number,
                      ssv.sample_site_purpose_type_code,
                      ssv.visit_number,
                      pd.*,
                      pt.*

                      from
                      app_ismc.plot_detail pd

                      left join app_ismc.plot pt
                      on pt.plot_guic = pd.plot_guic

                      left join app_ismc.sample_site_visit ssv
                      on ssv.sample_site_visit_guic = pd.sample_site_visit_guic

                      left join app_ismc.sample_site ss
                      on ss.sample_site_guic = ssv.sample_site_guic

                      left join app_ismc.ground_sample_project gsp
                      on gsp.ground_sample_project_guic = ssv.ground_sample_project_guic

                      where
                      ss.sample_site_name in ", siteName,
                      "order by
                      sample_site_name, visit_number, project_name, plot_category_code, plot_number")) %>%
    data.table
  PlotDetails <- cleanColumns(PlotDetails, level = "site_visit")

  SampleMeasurements <-
    dbGetQuery(con,
               paste0("select
                      ss.sample_site_name,
                      gsp.project_name,
                      gsp.project_number,
                      ssv.sample_site_purpose_type_code,
                      ssv.visit_number,
                      sm.*

                      from
                      app_ismc.sample_measurement sm

                      left join app_ismc.sample_site_visit ssv
                      on ssv.sample_site_visit_guic = sm.sample_site_visit_guic

                      left join app_ismc.ground_sample_project gsp
                      on gsp.ground_sample_project_guic = ssv.ground_sample_project_guic

                      left join app_ismc.sample_site ss
                      on ss.sample_site_guic = ssv.sample_site_guic

                      where
                      ss.sample_site_name in ", siteName,
                      "order by
                      sample_site_name, visit_number, project_name")) %>%
    data.table
  SampleMeasurements <- cleanColumns(SampleMeasurements, level = "site_visit")

  SmallLiveTreeTallies <-
    dbGetQuery(con,
               paste0("select
                      ss.sample_site_name,
                      pt.plot_category_code,
                      pt.plot_number,
                      ssv.visit_number,
                      gsp.project_name,
                      gsp.project_number,
                      ssv.sample_site_purpose_type_code,
                      sm.measurement_date,
                      sltt.*

                      from
                      app_ismc.small_live_tree_tally sltt

                      left join app_ismc.sample_measurement sm
                      on sm.sample_measurement_guic = sltt.sample_measurement_guic

                      left join app_ismc.sample_site_visit ssv
                      on ssv.sample_site_visit_guic = sm.sample_site_visit_guic

                      left join app_ismc.sample_site ss
                      on ss.sample_site_guic = ssv.sample_site_guic

                      left join app_ismc.ground_sample_project gsp
                      on gsp.ground_sample_project_guic = ssv.ground_sample_project_guic

                      left join app_ismc.plot pt
                      on pt.plot_guic = sm.plot_guic

                      where
                      ss.sample_site_name in ", siteName,
                      "order by
                      sample_site_name, plot_number, visit_number, tree_species_code, small_tree_tally_class_code")) %>%
    data.table
  SmallLiveTreeTallies <- cleanColumns(SmallLiveTreeTallies, level = "site_visit")




  TreeMeasurements <-
    dbGetQuery(con,
               paste0("select
                      ss.sample_site_name,
                      gsp.project_name,
                      gsp.project_number,
                      ssv.sample_site_purpose_type_code,
                      ssv.visit_number,
                      sm.measurement_date,
                      pt.plot_category_code,
                      pt.plot_number,
                      tr.*,
                      td.*,
                      tm.*,
                      vtm.*

                      from
                      app_ismc.tree_measurement tm

                      left join app_ismc.tree tr
                      on tr.tree_guic = tm.tree_guic

                      left join app_ismc.plot pt
                      on pt.plot_guic = tr.plot_guic

                      left join app_ismc.vri_tree_measurement vtm
                      on vtm.tree_measurement_guic = tm.tree_measurement_guic

                      left join app_ismc.sample_measurement sm
                      on sm.sample_measurement_guic = tm.sample_measurement_guic

                      left join app_ismc.sample_site_visit ssv
                      on ssv.sample_site_visit_guic = sm.sample_site_visit_guic

                      left join app_ismc.tree_detail td
                      on td.tree_guic = tm.tree_guic and
                      td.sample_site_visit_guic = sm.sample_site_visit_guic

                      left join app_ismc.sample_site ss
                      on ss.sample_site_guic = ssv.sample_site_guic

                      left join app_ismc.ground_sample_project gsp
                      on gsp.ground_sample_project_guic = ssv.ground_sample_project_guic

                      where
                      ss.sample_site_name in ", siteName,
                      "order by
                      sample_site_name, visit_number, plot_category_code, plot_number, tree_number")) %>%
    data.table
  TreeMeasurements <- cleanColumns(TreeMeasurements, level = "tree")
  TreeDamageOccurrences <-
    dbGetQuery(con,
               paste0("select
                      ss.sample_site_name,
                      gsp.project_name,
                      gsp.project_number,
                      ssv.sample_site_purpose_type_code,
                      ssv.visit_number,
                      sm.measurement_date,
                      pt.plot_category_code,
                      pt.plot_number,
                      tr.tree_number,
                      td.tree_species_code,
                      tm.diameter,
                      tm.length,
                      tdo.*,
                      das.*

                      from
                      app_ismc.tree_damage_occurrence tdo

                      left join app_ismc.damage_agent_severity das
                      on das.damage_agent_severity_guic = tdo.damage_agent_severity_guic

                      left join app_ismc.tree_measurement tm
                      on tm.tree_measurement_guic = tdo.tree_measurement_guic

                      left join app_ismc.sample_measurement sm
                      on sm.sample_measurement_guic = tm.sample_measurement_guic

                      left join app_ismc.sample_site_visit ssv
                      on ssv.sample_site_visit_guic = sm.sample_site_visit_guic

                      left join app_ismc.sample_site ss
                      on ss.sample_site_guic = ssv.sample_site_guic

                      left join app_ismc.ground_sample_project gsp
                      on gsp.ground_sample_project_guic = ssv.ground_sample_project_guic

                      left join app_ismc.tree tr
                      on tr.tree_guic = tm.tree_guic

                      left join app_ismc.plot pt
                      on pt.plot_guic = tr.plot_guic

                      left join app_ismc.tree_detail td on td.tree_guic = tm.tree_guic
                      and td.sample_site_visit_guic = sm.sample_site_visit_guic

                      where
                      ss.sample_site_name in ", siteName,
                      "order by
                      sample_site_name, visit_number, plot_category_code,
                      plot_number, tree_number, sequence_number")) %>%
    data.table
  TreeDamageOccurrences <- cleanColumns(TreeDamageOccurrences, level = "tree")

  TreeLossIndicators <-
    dbGetQuery(con,
               paste0("select
                      ss.sample_site_name,
                      gsp.project_name,
                      gsp.project_number,
                      ssv.sample_site_purpose_type_code,
                      ssv.visit_number,
                      sm.measurement_date,
                      pt.plot_category_code,
                      pt.plot_number,
                      tr.tree_number,
                      td.tree_species_code,
                      tm.diameter,
                      tm.length,
                      tli.*

                      from
                      app_ismc.tree_loss_indicator tli

                      left join app_ismc.tree_measurement tm
                      on tm.tree_measurement_guic = tli.tree_measurement_guic

                      left join app_ismc.sample_measurement sm
                      on sm.sample_measurement_guic = tm.sample_measurement_guic

                      left join app_ismc.sample_site_visit ssv
                      on ssv.sample_site_visit_guic = sm.sample_site_visit_guic

                      left join app_ismc.sample_site ss
                      on ss.sample_site_guic = ssv.sample_site_guic

                      left join app_ismc.ground_sample_project gsp
                      on gsp.ground_sample_project_guic = ssv.ground_sample_project_guic

                      left join app_ismc.tree tr
                      on tr.tree_guic = tm.tree_guic

                      left join app_ismc.plot pt
                      on pt.plot_guic = tr.plot_guic

                      left join app_ismc.tree_detail td
                      on td.tree_guic = tm.tree_guic and
                      td.sample_site_visit_guic = sm.sample_site_visit_guic

                      where
                      ss.sample_site_name in ", siteName,
                      "order by
                      sample_site_name, visit_number, plot_category_code,
                      plot_number, tree_number, location_from, location_to")) %>%
    data.table
  TreeLossIndicators <- cleanColumns(TreeLossIndicators, level = "tree")

  TreeLogAssessments <-
    dbGetQuery(con,
               paste0("select
                      ss.sample_site_name,
                      gsp.project_name,
                      gsp.project_number,
                      ssv.sample_site_purpose_type_code,
                      ssv.visit_number,
                      sm.measurement_date,
                      pt.plot_category_code,
                      pt.plot_number,
                      tr.tree_number,
                      td.tree_species_code,
                      tm.diameter,
                      tm.length,
                      la.*
                      from
                      app_ismc.log_assessment la

                      left join app_ismc.vri_tree_measurement vtm
                      on vtm.vri_tree_measurement_guic = la.vri_tree_measurement_guic

                      left join app_ismc.tree_measurement tm
                      on tm.tree_measurement_guic = vtm.tree_measurement_guic

                      left join app_ismc.sample_measurement sm
                      on sm.sample_measurement_guic = tm.sample_measurement_guic

                      left join app_ismc.sample_site_visit ssv
                      on ssv.sample_site_visit_guic = sm.sample_site_visit_guic

                      left join app_ismc.sample_site ss
                      on ss.sample_site_guic = ssv.sample_site_guic

                      left join app_ismc.ground_sample_project gsp
                      on gsp.ground_sample_project_guic = ssv.ground_sample_project_guic

                      left join app_ismc.tree tr
                      on tr.tree_guic = tm.tree_guic

                      left join app_ismc.plot pt
                      on pt.plot_guic = tr.plot_guic

                      left join app_ismc.tree_detail td
                      on td.tree_guic = tm.tree_guic and
                      td.sample_site_visit_guic = sm.sample_site_visit_guic

                      where
                      ss.sample_site_name in ", siteName,
                      "order by
                      sample_site_name, visit_number, plot_category_code,
                      plot_number, tree_number, log_number")) %>%
    data.table
  TreeLogAssessments <- cleanColumns(TreeLogAssessments, level = "tree")


  StumpTallies <-
    dbGetQuery(con,
               paste0("select
                      ss.sample_site_name,
                      gsp.project_name,
                      gsp.project_number,
                      ssv.sample_site_purpose_type_code,
                      ssv.visit_number,
                      sm.measurement_date,
                      st.*
                      from
                      app_ismc.stump_tally st

                      left join app_ismc.vri_sample_measurement vsm
                      on vsm.vri_sample_measurement_guic = st.vri_sample_measurement_guic

                      left join app_ismc.sample_measurement sm
                      on sm.sample_measurement_guic = vsm.sample_measurement_guic

                      left join app_ismc.sample_site_visit ssv
                      on ssv.sample_site_visit_guic = sm.sample_site_visit_guic

                      left join app_ismc.sample_site ss
                      on ss.sample_site_guic = ssv.sample_site_guic

                      left join app_ismc.ground_sample_project gsp
                      on gsp.ground_sample_project_guic = ssv.ground_sample_project_guic

                      where
                      ss.sample_site_name in ", siteName,
                      "order by
                      sample_site_name, visit_number")) %>%
    data.table
  StumpTallies <- cleanColumns(StumpTallies, level = "site_visit")

  SiteNavigation <-
    dbGetQuery(con,
               paste0("select
                      ss.sample_site_name,
                      gsp.project_name,
                      gsp.project_number,
                      ssv.sample_site_purpose_type_code,
                      ssv.visit_number,
                      sn.*
                      from
                      app_ismc.site_navigation sn

                      left join app_ismc.sample_site_visit ssv
                      on ssv.sample_site_visit_guic = sn.sample_site_visit_guic

                      left join app_ismc.sample_site ss
                      on ss.sample_site_guic = ssv.sample_site_guic

                      left join app_ismc.ground_sample_project gsp
                      on gsp.ground_sample_project_guic = ssv.ground_sample_project_guic

                      where
                      ss.sample_site_name in ", siteName,
                      "order by
                      sample_site_name, visit_number")) %>%
    data.table
  SiteNavigation <- cleanColumns(SiteNavigation, level = "site_visit")

  IntegratedPlotCenter <-
    dbGetQuery(con,
               paste0("select
                      ss.sample_site_name,
                      gsp.project_name,
                      gsp.project_number,
                      ssv.sample_site_purpose_type_code,
                      ssv.visit_number,
                      ipc.*,
                      pl.utm_zone,
                      pl.utm_northing,
                      pl.utm_easting,
                      pl.elevation,
                      pl.coordinate_source_code
                      from
                      app_ismc.integrated_plot_center ipc

                      left join app_ismc.site_navigation sn
                      on sn.site_navigation_guic = ipc.site_navigation_guic

                      left join app_ismc.point_location pl
                      on pl.point_location_guic = ipc.point_location_guic

                      left join app_ismc.sample_site_visit ssv
                      on ssv.sample_site_visit_guic = sn.sample_site_visit_guic

                      left join app_ismc.sample_site ss
                      on ss.sample_site_guic = ssv.sample_site_guic

                      left join app_ismc.ground_sample_project gsp
                      on gsp.ground_sample_project_guic = ssv.ground_sample_project_guic

                      where
                      ss.sample_site_name in ", siteName,
                      "order by
                      sample_site_name, visit_number")) %>%
    data.table
  IntegratedPlotCenter <- cleanColumns(IntegratedPlotCenter, level = "site_visit")


  ReferencePoint <-
    dbGetQuery(con,
               paste0("select
                      ss.sample_site_name,
                      gsp.project_name,
                      gsp.project_number,
                      ssv.sample_site_purpose_type_code,
                      ssv.visit_number,
                      rfp.*,
                      rff.*

                      from
                      app_ismc.reference_point rfp

                      left join app_ismc.site_navigation sn
                      on sn.site_navigation_guic = rfp.site_navigation_guic

                      left join app_ismc.reference_feature rff
                      on rff.reference_point_guic = rfp.reference_point_guic

                      left join app_ismc.sample_site_visit ssv
                      on ssv.sample_site_visit_guic = sn.sample_site_visit_guic

                      left join app_ismc.sample_site ss
                      on ss.sample_site_guic = ssv.sample_site_guic

                      left join app_ismc.ground_sample_project gsp
                      on gsp.ground_sample_project_guic = ssv.ground_sample_project_guic

                      where
                      ss.sample_site_name in ", siteName,
                      "order by
                      sample_site_name, visit_number")) %>%
    data.table
  ReferencePoint <- cleanColumns(ReferencePoint, level = "site_visit")

  TiePoint <-
    dbGetQuery(con,
               paste0("select
                      ss.sample_site_name,
                      gsp.project_name,
                      gsp.project_number,
                      ssv.sample_site_purpose_type_code,
                      ssv.visit_number,
                      tpt.*,
                      rff.*,
                      plc.utm_zone,
                      plc.utm_northing,
                      plc.utm_easting,
                      plc.elevation,
                      plc.coordinate_source_code

                      from
                      app_ismc.tie_point tpt

                      left join app_ismc.site_navigation sn
                      on sn.site_navigation_guic = tpt.site_navigation_guic

                      left join app_ismc.reference_feature rff
                      on rff.tie_point_guic = tpt.tie_point_guic

                      left join app_ismc.point_location plc
                      on plc.point_location_guic = tpt.point_location_guic

                      left join app_ismc.sample_site_visit ssv
                      on ssv.sample_site_visit_guic = sn.sample_site_visit_guic

                      left join app_ismc.sample_site ss
                      on ss.sample_site_guic = ssv.sample_site_guic

                      left join app_ismc.ground_sample_project gsp
                      on gsp.ground_sample_project_guic = ssv.ground_sample_project_guic

                      where
                      ss.sample_site_name in ", siteName,
                      "order by
                      sample_site_name, visit_number")) %>%
    data.table
  TiePoint <- cleanColumns(TiePoint, level = "site_visit")
  dbDisconnect(con)

  savefiles <- c("SampleSites", "AccessNotes", "PlotDetails",
                 "SampleSiteVisits", "SampleMeasurements",
                 "GroundSampleCrewActivities", "SiteNavigation",
                 "IntegratedPlotCenter",
                 "ReferencePoint", "TiePoint", "SmallLiveTreeTallies",
                 "StumpTallies", "TreeMeasurements",
                 "TreeLogAssessments", "TreeDamageOccurrences",
                 "TreeLossIndicators")
  if(saveFormat == "xlsx"){
    testwb <- openxlsx::createWorkbook()
  }
  for(indifile in savefiles){
    cat(indifile, "\n")
    indifiledata <- get(indifile)
    if(saveFormat == "xlsx"){
      openxlsx::addWorksheet(testwb, indifile)
      openxlsx::writeData(testwb, indifile, indifiledata)
    } else if(saveFormat %in% c("rds", "csv", "txt")){
      writeISMC(savePath = savePath, saveName = saveName,
                tableName = indifile, saveFormat = saveFormat,
                thedata = indifiledata)
    } else if(saveFormat != "rdata"){
      stop("Output format has not been correctly specified.")
    }
    rm(indifile, indifiledata)
  }
  if(saveFormat == "rdata"){
    save(list = savefiles,
         file = file.path(savePath, paste0(saveName, ".rdata")))
  } else if (saveFormat == "xlsx"){
    openxlsx::saveWorkbook(testwb,
                           file = file.path(savePath, paste0(saveName, ".xlsx")),
                           overwrite = TRUE)
  }
}

