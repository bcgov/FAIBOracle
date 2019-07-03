#' Load the data from ISMC Oracle database using sample type
#'
#'
#' @description This function is to load the ground sample data from ISMC database using
#'              sample type.
#'
#' @param userName character, Specifies a valid user name in ISMC Oracle database.
#' @param passWord character, Specifies the password to the user name.
#' @param savePath character, Specifies the path to save your outputs, you do not need to
#'                 specify if \code{saveThem} is turned off. If missing, the current working
#'                 directory will be choosed.
#'
#' @param saveFormat character, Specifies the format for the output data.
#'                   It accepts \code{xlsx}, \code{csv} and \code{rds}. Default is \code{rds}.
#' @param overWrite logical, Determine if the file with same name as user specifies
#'                           will be overwritten. Default is \code{FALSE}.
#'
#' @return no value returned. There are 17 tables will be saved based on user's specifications.
#'         They are SampleSites, AccessNotes, PointLocation, PlotDetails,
#'         SampleSiteVisits, SampleMeasurements,
#'         GroundSampleCrewActivities, SiteNavigation,
#'         IntegratedPlotCenter, ReferencePoint, TiePoint,
#'         SmallLiveTreeTallies, StumpTallies,
#'         TreeMeasurements, TreeLogAssessments,
#'         TreeDamageOccurrences and TreeLossIndicators.
#'
#' @importFrom data.table ':=' data.table
#' @importFrom dplyr '%>%'
#' @importFrom ROracle dbConnect dbGetQuery dbDisconnect
#' @importFrom DBI dbDriver
#' @importFrom openxlsx write.xlsx
#'
#' @rdname loadISMC_all
#' @author Yong Luo
loadISMC_all <- function(userName, passWord,
                         savePath = ".",
                         saveFormat = "rds",
                         overWrite = FALSE){
  thetime <- substr(as.character(Sys.time()), 12, 13)
  if(as.integer(thetime) < 12){
    thetime <- paste0(thetime, "am")
  } else {
    thetime <- paste0(thetime, "pm")
  }
  saveName <- paste0("ISMC_", substr(as.character(Sys.time()), 1, 10), thetime, "(ALL)")
  saveName <- gsub("-", "", saveName)

  drv <- dbDriver("Oracle")
  connect_to_ismc <- "(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=TCP)
  (HOST=nrc1-scan.bcgov)(PORT=1521)))
  (CONNECT_DATA=(SERVICE_NAME=ismcint.nrs.bcgov)))"
  con <- dbConnect(drv, username = userName,
                   password = passWord,
                   dbname = connect_to_ismc)
  SampleSites <-
    dbGetQuery(con,
               "select
               ss.*

               from
               app_ismc.sample_site ss") %>%
    data.table
SampleSites <- unlistGUID(SampleSites)
  writeISMC(savePath = savePath, saveName = saveName,
               tableName = "SampleSites", saveFormat = saveFormat,
               thedata = SampleSites)
  rm(SampleSites)
  gc()

  AccessNotes <-
    dbGetQuery(con,
               "select
               an.*

               from
               app_ismc.access_note an") %>%
    data.table
  AccessNotes <- unlistGUID(AccessNotes)
  writeISMC(savePath = savePath, saveName = saveName,
               tableName = "AccessNotes", saveFormat = saveFormat,
               thedata = AccessNotes)
  rm(AccessNotes)
  gc()

  Plots <-
    dbGetQuery(con,
               "select
               pt.*

               from
               app_ismc.plot pt") %>%
    data.table
  Plots <- unlistGUID(Plots)
  writeISMC(savePath = savePath, saveName = saveName,
               tableName = "Plots", saveFormat = saveFormat,
               thedata = Plots)
  rm(Plots)
  gc()




  SampleSiteVisits <-
    dbGetQuery(con,
               "select
               ssv.*

               from
               app_ismc.sample_site_visit ssv") %>%
    data.table
  SampleSiteVisits <- unlistGUID(SampleSiteVisits)
  writeISMC(savePath = savePath, saveName = saveName,
               tableName = "SampleSiteVisits", saveFormat = saveFormat,
               thedata = SampleSiteVisits)
  rm(SampleSiteVisits)
  gc()


  GroundSampleCrewActivities <-
    dbGetQuery(con,
               "select
               gsca.*

               from
               app_ismc.ground_sample_crew_actvty gsca") %>%
    data.table
  GroundSampleCrewActivities <- unlistGUID(GroundSampleCrewActivities)
  writeISMC(savePath = savePath, saveName = saveName,
               tableName = "GroundSampleCrewActivities",
               saveFormat = saveFormat,
               thedata = GroundSampleCrewActivities)
  rm(GroundSampleCrewActivities)
  gc()


  PlotDetails <-
    dbGetQuery(con,
               "select
               pd.*

               from
               app_ismc.plot_detail pd") %>%
    data.table
  PlotDetails <- unlistGUID(PlotDetails)
  writeISMC(savePath = savePath, saveName = saveName,
               tableName = "PlotDetails", saveFormat = saveFormat,
               thedata = PlotDetails)
  rm(PlotDetails)
  gc()


  SampleMeasurements <-
    dbGetQuery(con,
               "select
               sm.*

               from
               app_ismc.sample_measurement sm") %>%
    data.table
  SampleMeasurements <- unlistGUID(SampleMeasurements)
  writeISMC(savePath = savePath, saveName = saveName,
               tableName = "SampleMeasurements", saveFormat = saveFormat,
               thedata = SampleMeasurements)
  rm(SampleMeasurements)
  gc()


  SmallLiveTreeTallies <-
    dbGetQuery(con,
               "select
               sltt.*

               from
               app_ismc.small_live_tree_tally sltt") %>%
    data.table
  SmallLiveTreeTallies <- unlistGUID(SmallLiveTreeTallies)
  writeISMC(savePath = savePath, saveName = saveName,
               tableName = "SmallLiveTreeTallies", saveFormat = saveFormat,
               thedata = SmallLiveTreeTallies)
  rm(SmallLiveTreeTallies)
  gc()


  TreeMeasurements <-
    dbGetQuery(con,
               "select
               tm.*

               from
               app_ismc.tree_measurement tm") %>% data.table
  TreeMeasurements <- unlistGUID(TreeMeasurements)
  writeISMC(savePath = savePath, saveName = saveName,
               tableName = "TreeMeasurements", saveFormat = saveFormat,
               thedata = TreeMeasurements)
  rm(TreeMeasurements)
  gc()


  Trees <-
    dbGetQuery(con,
               "select
               tr.*

               from
               app_ismc.tree tr") %>% data.table
  Trees <- unlistGUID(Trees)
  writeISMC(savePath = savePath, saveName = saveName,
               tableName = "Trees", saveFormat = saveFormat,
               thedata = Trees)
  rm(Trees)
  gc()


  TreeDetails <-
    dbGetQuery(con,
               "select
               td.*

               from
               app_ismc.tree_detail td") %>% data.table
  TreeDetails <- unlistGUID(TreeDetails)
  writeISMC(savePath = savePath, saveName = saveName,
               tableName = "TreeDetails", saveFormat = saveFormat,
               thedata = TreeDetails)
  rm(TreeDetails)
  gc()


  TreeDamageOccurrences <-
    dbGetQuery(con,
               "select
               tdo.*

               from
               app_ismc.tree_damage_occurrence tdo") %>%
    data.table
  TreeDamageOccurrences <- unlistGUID(TreeDamageOccurrences)
  writeISMC(savePath = savePath, saveName = saveName,
               tableName = "TreeDamageOccurrences", saveFormat = saveFormat,
               thedata = TreeDamageOccurrences)
  rm(TreeDamageOccurrences)
  gc()


  TreeLossIndicators <-
    dbGetQuery(con,
               "select
               tli.*

               from
               app_ismc.tree_loss_indicator tli") %>%
    data.table
  TreeLossIndicators <- unlistGUID(TreeLossIndicators)
  writeISMC(savePath = savePath, saveName = saveName,
               tableName = "TreeLossIndicators", saveFormat = saveFormat,
               thedata = TreeLossIndicators)
  rm(TreeLossIndicators)
  gc()


  TreeLogAssessments <-
    dbGetQuery(con,
               "select
               la.*
               from
               app_ismc.log_assessment la") %>%
    data.table
  TreeLogAssessments <- unlistGUID(TreeLogAssessments)
  writeISMC(savePath = savePath, saveName = saveName,
               tableName = "TreeLogAssessments", saveFormat = saveFormat,
               thedata = TreeLogAssessments)
  rm(TreeLogAssessments)
  gc()


  StumpTallies <-
    dbGetQuery(con,
               "select
               st.*

               from
               app_ismc.stump_tally st") %>%
    data.table
  StumpTallies <- unlistGUID(StumpTallies)
  writeISMC(savePath = savePath, saveName = saveName,
               tableName = "StumpTallies", saveFormat = saveFormat,
               thedata = StumpTallies)
  rm(StumpTallies)
  gc()


  SiteNavigation <-
    dbGetQuery(con,
               "select
               sn.*
               from
               app_ismc.site_navigation sn") %>%
    data.table
  SiteNavigation <- unlistGUID(SiteNavigation)
  writeISMC(savePath = savePath, saveName = saveName,
               tableName = "SiteNavigation", saveFormat = saveFormat,
               thedata = SiteNavigation)
  rm(SiteNavigation)
  gc()


  IntegratedPlotCenter <-
    dbGetQuery(con,
               "select
               ipc.*

               from
               app_ismc.integrated_plot_center ipc") %>%
    data.table
  IntegratedPlotCenter <- unlistGUID(IntegratedPlotCenter)
  writeISMC(savePath = savePath, saveName = saveName,
               tableName = "IntegratedPlotCenter", saveFormat = saveFormat,
               thedata = IntegratedPlotCenter)
  rm(IntegratedPlotCenter)
  gc()


  ReferencePoint <-
    dbGetQuery(con,
               "select
               rfp.*

               from
               app_ismc.reference_point rfp") %>%
    data.table
  ReferencePoint <- unlistGUID(ReferencePoint)
  writeISMC(savePath = savePath, saveName = saveName,
               tableName = "ReferencePoint", saveFormat = saveFormat,
               thedata = ReferencePoint)
  rm(ReferencePoint)
  gc()


  TiePoint <-
    dbGetQuery(con,
               "select
               tpt.*

               from
               app_ismc.tie_point tpt") %>%
    data.table
  TiePoint <- unlistGUID(TiePoint)
  writeISMC(savePath = savePath, saveName = saveName,
               tableName = "TiePoint", saveFormat = saveFormat,
               thedata = TiePoint)
  rm(TiePoint)
  gc()
  dbDisconnect(con)
}




