#' Load the data from ISMC Oracle database using sample type(s)
#'
#'
#' @description This function is to load the ground sample data from ISMC database using
#'              sample type(s).
#'
#' @param userName character, Specifies a valid user name in ISMC Oracle database.
#' @param passWord character, Specifies the password to the user name.
#' @param env character, Specifies which environment the data reside. Currently,
#'                               the function supports \code{INT} (intergration)
#'                               and \code {TEST} (test) environment.
#' @param sampleType character, Site type or types for BC ground sample inventory, such as \code{L}, \code{M} and \code{Y}.
#' @param savePath character, Specifies the path to save your outputs. If missing, the current working
#'                 directory will be choosed.
#'
#' @param saveFormat character, Specifies the format for the output data.
#'                   It accepts \code{xlsx} and \code{rdata}. Default is \code{rdata}.
#' @param overWrite logical, Determine if the file with same name as user specifies
#'                           will be overwritten. Default is \code{FALSE}.
#'
#' @return No value returned. There are 16 tables will be saved with prefix of \code{ISMC_YYYYMMDDHHHH(sampletype)_}.
#'         These tables are SampleSites, AccessNotes,
#'         SampleSiteVisits, GroundSampleCrewActivities, PlotDetails, SampleMeasurements,
#'         SmallLiveTreeTallies, TreeMeasurements, Trees, TreeDetails,
#'         TreeDamageOccurrences, TreeLossIndicators, TreeLogAssessments,
#'         StumpTallies, SiteNavigation,
#'         IntegratedPlotCenter, ReferencePoint and TiePoint.
#'
#' @importFrom data.table ':=' data.table
#' @importFrom dplyr '%>%'
#' @importFrom ROracle dbConnect dbGetQuery dbDisconnect
#' @importFrom DBI dbDriver
#' @importFrom openxlsx createWorkbook addWorksheet writeData saveWorkbook
#' @importFrom utils write.csv write.table
#' @export
#'
#' @rdname loadISMC_bySampleType
#' @author Yong Luo
loadISMC_bySampleType <- function(userName, passWord, env,
                                  sampleType,
                                  savePath = ".",
                                  saveFormat = "rdata",
                                  overWrite = FALSE){
  thetime <- substr(as.character(Sys.time()), 12, 13)
  if(as.integer(thetime) < 12){
    thetime <- paste0(thetime, "am")
  } else {
    thetime <- paste0(thetime, "pm")
  }
  saveName <- paste0("ISMC_", substr(as.character(Sys.time()), 1, 10), thetime,
                     "(", paste0(sampleType, collapse = "_"), ")")
  saveName <- gsub("-", "", saveName)



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
  sampleType_org <- sampleType
  if("PSP" %in% sampleType_org & saveFormat %in% c("xlsx")){
    stop("Tree measurement dataset is too big to save using xlsx for PSP data")
  }


  if(length(sampleType) == 1){
    sampleType <- paste0("('", sampleType,"')")
  } else {

    sampleType <- paste0("('", paste0(sampleType, collapse = "', '"),"')")
  }

  drv <- dbDriver("Oracle")
  if(env == "TEST"){
    connect_to_ismc <- "(DESCRIPTION = (ADDRESS_LIST = (ADDRESS = (PROTOCOL = TCP)
    (HOST = nrcdb01.bcgov)(PORT = 1521)))
    (CONNECT_DATA = (SERVICE_NAME = ISMCTST.NRS.BCGOV)))"
  } else if (env == "INT"){
    connect_to_ismc <- "(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=TCP)
    (HOST=nrcdb01.bcgov)(PORT=1521)))
    (CONNECT_DATA=(SERVICE_NAME=ismcint.nrs.bcgov)))"
  } else {
    stop("env must be specified either INT or TEST.")
  }
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

                      -- rcl_mvw is materialized view
                      left join app_ismc.ismc_rcl_mvw rcl
                      on rcl.areal_unit_guic = pspss.areal_unit_guic

                      where exists (
                      --
                      -- revised based on John's comment
                      --
                      select 1

                      from app_ismc.sample_site_visit ssv
                      where ssv.sample_site_guic = ss.sample_site_guic

                      and ssv.sample_site_purpose_type_code in ", sampleType,
                      ")
                      order by
                      sample_site_name")) %>%
    data.table

  SampleSites <- cleanColumns(SampleSites, level = "sample_site")
  SampleSites <- unique(SampleSites)
  writeISMC(savePath = savePath, saveName = saveName,
            tableName = "SampleSites", saveFormat = saveFormat,
            thedata = SampleSites)

  if(nrow(SampleSites) == 0){
    stop(paste0("There is no record for the sample type(s): ",
                paste0(sampleType_org, collapse = ", ")))
  }
  rm(SampleSites)
  gc()

  AccessNotes <-
    dbGetQuery(con,
               paste0("select
                      ss.sample_site_name,
                      an.*

                      from
                      app_ismc.access_note an

                      left join app_ismc.sample_site ss
                      on ss.sample_site_guic = an.sample_site_guic

                      where exists (
                      --
                      -- revised based on John's comment
                      --
                      select 1

                      from app_ismc.sample_site_visit ssv
                      where ssv.sample_site_guic = ss.sample_site_guic

                      and ssv.sample_site_purpose_type_code in ", sampleType,
                      ")
                      order by
                      sample_site_name, sequence_number")) %>%
    data.table
  AccessNotes <- cleanColumns(AccessNotes, level = "sample_site")
  AccessNotes <- unique(AccessNotes)

  writeISMC(savePath = savePath, saveName = saveName,
            tableName = "AccessNotes", saveFormat = saveFormat,
            thedata = AccessNotes)
  rm(AccessNotes)
  gc()


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
                      ssv.sample_site_purpose_type_code in ", sampleType,
                      "order by
                      sample_site_name, visit_number, project_name, sample_site_purpose_type_code")) %>%
    data.table
  SampleSiteVisits <- cleanColumns(SampleSiteVisits, level = "site_visit")
  writeISMC(savePath = savePath, saveName = saveName,
            tableName = "SampleSiteVisits", saveFormat = saveFormat,
            thedata = SampleSiteVisits)
  rm(SampleSiteVisits)
  gc()



  GroundSampleCrewActivities <-
    dbGetQuery(con,
               paste0("select
                      ss.sample_site_name,
                      ssv.visit_number,
                      gsp.project_name,
                      gsp.project_number,
                      ssv.sample_site_purpose_type_code,
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
                      ssv.sample_site_purpose_type_code in ", sampleType,
                      "order by
                      sample_site_name, visit_number, project_name")) %>%
    data.table
  GroundSampleCrewActivities <- cleanColumns(GroundSampleCrewActivities, level = "site_visit")
  writeISMC(savePath = savePath, saveName = saveName,
            tableName = "GroundSampleCrewActivities", saveFormat = saveFormat,
            thedata = GroundSampleCrewActivities)
  rm(GroundSampleCrewActivities)
  gc()


  PlotDetails <-
    dbGetQuery(con,
               paste0("select
                      ss.sample_site_name,
                      ssv.visit_number,
                      gsp.project_name,
                      gsp.project_number,
                      ssv.sample_site_purpose_type_code,
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
                      ssv.sample_site_purpose_type_code in ", sampleType,
                      "order by
                      sample_site_name, visit_number, project_name, plot_category_code, plot_number")) %>%
    data.table
  PlotDetails <- cleanColumns(PlotDetails, level = "site_visit")
  writeISMC(savePath = savePath, saveName = saveName,
            tableName = "PlotDetails", saveFormat = saveFormat,
            thedata = PlotDetails)
  rm(PlotDetails)
  gc()

  SampleMeasurements <-
    dbGetQuery(con,
               paste0("select
                      ss.sample_site_name,
                      ssv.visit_number,
                      pt.plot_category_code,
                      pt.plot_number,
                      gsp.project_name,
                      gsp.project_number,
                      ssv.sample_site_purpose_type_code,
                      sm.*

                      from
                      app_ismc.sample_measurement sm

                      left join app_ismc.sample_site_visit ssv
                      on ssv.sample_site_visit_guic = sm.sample_site_visit_guic

                      left join app_ismc.plot pt
                      on pt.plot_guic = sm.plot_guic

                      left join app_ismc.ground_sample_project gsp
                      on gsp.ground_sample_project_guic = ssv.ground_sample_project_guic

                      left join app_ismc.sample_site ss
                      on ss.sample_site_guic = ssv.sample_site_guic

                      where
                      ssv.sample_site_purpose_type_code in ", sampleType,
                      "order by
                      sample_site_name, visit_number, project_name")) %>%
    data.table
  SampleMeasurements <- cleanColumns(SampleMeasurements, level = "site_visit")
  writeISMC(savePath = savePath, saveName = saveName,
            tableName = "SampleMeasurements", saveFormat = saveFormat,
            thedata = SampleMeasurements)
  rm(SampleMeasurements)
  gc()


  SmallLiveTreeTallies <-
    dbGetQuery(con,
               paste0("select
                      ss.sample_site_name,
                      ssv.visit_number,
                      pt.plot_category_code,
                      pt.plot_number,
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
                      ssv.sample_site_purpose_type_code in ", sampleType,
                      "order by
                      sample_site_name, visit_number, plot_number, tree_species_code, small_tree_tally_class_code")) %>%
    data.table
  SmallLiveTreeTallies <- cleanColumns(SmallLiveTreeTallies, level = "site_visit")
  writeISMC(savePath = savePath, saveName = saveName,
            tableName = "SmallLiveTreeTallies", saveFormat = saveFormat,
            thedata = SmallLiveTreeTallies)
  rm(SmallLiveTreeTallies)
  gc()

  TreeMeasurements <-
    dbGetQuery(con,
               paste0("select
                      ss.sample_site_name,
                      ssv.visit_number,
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
                      ssv.sample_site_purpose_type_code in ", sampleType,
                      "order by
                      sample_site_name, visit_number, plot_category_code, plot_number, tree_number"))
  gc()
  chunckNum <- as.integer(nrow(TreeMeasurements)/1000000)

  for(i in 0:chunckNum){
    if(i != chunckNum){
      tm_chuncked <- TreeMeasurements[((1000000*i+1) : (1000000*(i+1))),]
      tm_chuncked <- data.table(tm_chuncked)
      tm_chuncked <- cleanColumns(tm_chuncked, level = "tree")
      gc()
    } else {
      tm_chuncked <- TreeMeasurements[((1000000*i+1) : nrow(TreeMeasurements)),]
      tm_chuncked <- data.table(tm_chuncked)
      tm_chuncked <- cleanColumns(tm_chuncked, level = "tree")
      gc()
    }

    if(i == 0){
      newtreedata <- data.table::copy(tm_chuncked)
      rm(tm_chuncked)
      gc()
    } else {
      newtreedata <- data.table::rbindlist(list(newtreedata, tm_chuncked))
      rm(tm_chuncked)
      gc()
    }
  }
  rm(TreeMeasurements)
  gc()
  writeISMC(savePath = savePath, saveName = saveName,
            tableName = "TreeMeasurements", saveFormat = saveFormat,
            thedata = newtreedata)
  rm(newtreedata)
  gc()

  TreeDamageOccurrences <-
    dbGetQuery(con,
               paste0("select
                      ss.sample_site_name,
                      ssv.visit_number,
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
                      ssv.sample_site_purpose_type_code in ", sampleType,
                      "order by
                      sample_site_name, visit_number, plot_category_code,
                      plot_number, tree_number, sequence_number")) %>%
    data.table

  TreeDamageOccurrences <- cleanColumns(TreeDamageOccurrences, level = "tree")
  writeISMC(savePath = savePath, saveName = saveName,
            tableName = "TreeDamageOccurrences", saveFormat = saveFormat,
            thedata = TreeDamageOccurrences)
  rm(TreeDamageOccurrences)
  gc()




  TreeLossIndicators <-
    dbGetQuery(con,
               paste0("select
                      ss.sample_site_name,
                      ssv.visit_number,
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
                      ssv.sample_site_purpose_type_code in ", sampleType,
                      "order by
                      sample_site_name, visit_number, plot_category_code,
                      plot_number, tree_number, location_from, location_to")) %>%
    data.table
  TreeLossIndicators <- cleanColumns(TreeLossIndicators, level = "tree")
  writeISMC(savePath = savePath, saveName = saveName,
            tableName = "TreeLossIndicators", saveFormat = saveFormat,
            thedata = TreeLossIndicators)
  rm(TreeLossIndicators)
  gc()



  TreeLogAssessments <-
    dbGetQuery(con,
               paste0("select
                      ss.sample_site_name,
                      ssv.visit_number,
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
                      ssv.sample_site_purpose_type_code in ", sampleType,
                      "order by
                      sample_site_name, visit_number, plot_category_code,
                      plot_number, tree_number, log_number")) %>%
    data.table
  TreeLogAssessments <- cleanColumns(TreeLogAssessments, level = "tree")
  writeISMC(savePath = savePath, saveName = saveName,
            tableName = "TreeLogAssessments", saveFormat = saveFormat,
            thedata = TreeLogAssessments)
  rm(TreeLogAssessments)
  gc()



  StumpTallies <-
    dbGetQuery(con,
               paste0("select
                      ss.sample_site_name,
                      ssv.visit_number,
                      gsp.project_name,
                      gsp.project_number,
                      ssv.sample_site_purpose_type_code,
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
                      ssv.sample_site_purpose_type_code in ", sampleType,
                      "order by
                      sample_site_name, visit_number")) %>%
    data.table
  StumpTallies <- cleanColumns(StumpTallies, level = "site_visit")
  writeISMC(savePath = savePath, saveName = saveName,
            tableName = "StumpTallies", saveFormat = saveFormat,
            thedata = StumpTallies)
  rm(StumpTallies)
  gc()


  SiteNavigation <-
    dbGetQuery(con,
               paste0("select
                      ss.sample_site_name,
                      ssv.visit_number,
                      gsp.project_name,
                      gsp.project_number,
                      ssv.sample_site_purpose_type_code,
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
                      ssv.sample_site_purpose_type_code in ", sampleType,
                      "order by
                      sample_site_name, visit_number")) %>%
    data.table
  SiteNavigation <- cleanColumns(SiteNavigation, level = "site_visit")
  writeISMC(savePath = savePath, saveName = saveName,
            tableName = "SiteNavigation", saveFormat = saveFormat,
            thedata = SiteNavigation)
  rm(SiteNavigation)
  gc()

  IntegratedPlotCenter <-
    dbGetQuery(con,
               paste0("select
                      ss.sample_site_name,
                      ssv.visit_number,
                      gsp.project_name,
                      gsp.project_number,
                      ssv.sample_site_purpose_type_code,
                      ipc.*,
                      pl.utm_zone,
                      pl.utm_northing,
                      pl.utm_easting,
                      pl.elevation,
                      pl.point_location_type_code,
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
                      ssv.sample_site_purpose_type_code in ", sampleType,
                      "order by
                      sample_site_name, visit_number")) %>%
    data.table
  IntegratedPlotCenter <- cleanColumns(IntegratedPlotCenter, level = "site_visit")
  writeISMC(savePath = savePath, saveName = saveName,
            tableName = "IntegratedPlotCenter", saveFormat = saveFormat,
            thedata = IntegratedPlotCenter)
  rm(IntegratedPlotCenter)
  gc()

  ReferencePoint <-
    dbGetQuery(con,
               paste0("select
                      ss.sample_site_name,
                      ssv.visit_number,
                      gsp.project_name,
                      gsp.project_number,
                      ssv.sample_site_purpose_type_code,
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
                      ssv.sample_site_purpose_type_code in ", sampleType,
                      "order by
                      sample_site_name, visit_number")) %>%
    data.table
  ReferencePoint <- cleanColumns(ReferencePoint, level = "site_visit")
  writeISMC(savePath = savePath, saveName = saveName,
            tableName = "ReferencePoint", saveFormat = saveFormat,
            thedata = ReferencePoint)
  rm(ReferencePoint)
  gc()

  TiePoint <-
    dbGetQuery(con,
               paste0("select
                      ss.sample_site_name,
                      ssv.visit_number,
                      gsp.project_name,
                      gsp.project_number,
                      ssv.sample_site_purpose_type_code,
                      tpt.*,
                      rff.*,
                      plc.utm_zone,
                      plc.utm_northing,
                      plc.utm_easting,
                      plc.elevation,
                      plc.point_location_type_code,
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
                      ssv.sample_site_purpose_type_code in ", sampleType,
                      "order by
                      sample_site_name, visit_number")) %>%
    data.table
  TiePoint <- cleanColumns(TiePoint, level = "site_visit")
  writeISMC(savePath = savePath, saveName = saveName,
            tableName = "TiePoint", saveFormat = saveFormat,
            thedata = TiePoint)
  rm(TiePoint)
  gc()
  dbDisconnect(con)
}


cleanColumns <- function(thedata, level){
  indifiledata_names <- names(thedata)
  indifiledata_names_noid <- indifiledata_names[-grep("_GUIC", indifiledata_names)]
  if(length(indifiledata_names_noid) == 0){
    indifiledata_names_noid <- indifiledata_names
  }
  indifiledata_names_noid <-
    indifiledata_names_noid[!(indifiledata_names_noid %in% c("CREATE_DATE", "CREATE_USER",
                                                             "UPDATE_DATE", "UPDATE_USER",
                                                             "REVISION_COUNT"))]
  thedata <- thedata[,c(indifiledata_names_noid), with = FALSE]
  if(level == "sample_site"){
    frontnames <- c("SAMPLE_SITE_NAME")
  } else if (level == "site_visit"){
    frontnames <- c("SAMPLE_SITE_NAME", "VISIT_NUMBER", "PROJECT_NAME",
                    "PROJECT_NUMBER", "SAMPLE_SITE_PURPOSE_TYPE_CODE")
  } else if (level == "tree"){
    frontnames <- c("SAMPLE_SITE_NAME", "VISIT_NUMBER",
                    "PLOT_CATEGORY_CODE", "PLOT_NUMBER", "TREE_NUMBER")
  }
  restnames <- indifiledata_names_noid[!(indifiledata_names_noid %in% frontnames)]
  thedata <- thedata[,c(frontnames, restnames), with = FALSE]
  return(thedata)
}


writeISMC <- function(savePath, saveName, tableName, saveFormat, thedata){
  if(file.exists(file.path(savePath, paste0(saveName, "_", tableName, ".", saveFormat)))){
    warning(paste0("The original file, ", paste0(saveName, "_", tableName, ".", saveFormat),
                   ", has been overwritten."))
  }
  if(saveFormat == "rds"){
    saveRDS(thedata,
            file.path(savePath, paste0(saveName, "_", tableName, ".rds")))
  } else if (saveFormat == "csv"){
    write.csv(thedata,
              file.path(savePath, paste0(saveName, "_", tableName, ".csv")),
              row.names = FALSE)
  } else if (saveFormat == "xlsx") {
    openxlsx::write.xlsx(thedata,
                         file.path(savePath, paste0(saveName, "_", tableName, ".xlsx")),
                         row.names = FALSE)
  } else if (saveFormat == "txt"){
    write.table(thedata,
                file.path(savePath, paste0(saveName, "_", tableName, ".txt")),
                sep = "|",
                na = "",
                row.names = FALSE)
  } else {
    stop("Output format has not been correctly specified.")
  }
}
