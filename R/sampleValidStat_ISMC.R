#' Obtain sample validation status by measurement year, sample type and sample project
#' @description This function is to load all the table names from ISMC database.
#'
#' @param userName character, Specifies a valid user name in ISMC Oracle database.
#' @param passWord character, Specifies the password to the user name.
#' @param env character, Specifies which environment the data reside. Currently,
#'                               the function supports \code{INT} (intergration)
#'                               and \code {TEST} (test) environment.
#' @param sampleYear integer, Sample measurement year. If missing, the function takes
#'                            all the samples regardless of measurement year.
#' @param sampleType character, Sample type. If missing, the function takes
#'                            all the samples regardless of sample type.
#' @param sampleProject character, Sample project. If missing, the function takes
#'                            all the samples regardless of sample project.
#'
#' @return a data table
#'
#' @importFrom data.table ':=' data.table
#' @importFrom dplyr '%>%'
#' @importFrom ROracle dbConnect dbGetQuery dbDisconnect dbListTables
#' @importFrom DBI dbDriver
#' @export
#'
#' @seealso \code{\link{tablesInISMC}}
#' @rdname sampleValidStat_ISMC
#' @author Yong Luo
sampleValidStat_ISMC <- function(userName, passWord, env,
                                 sampleYear = NA,
                                 sampleType = NA,
                                 sampleProject = NA){
  wherestatement <- c()
  if(length(sampleYear) == 1){
    if(!is.na(sampleYear)){
      sampleYear <- paste0("('", sampleYear,"')")
      wherestatement <- c(wherestatement,
                          paste0("extract(year from ssv.sample_site_visit_start_date) in ",
                                 sampleYear))
    }
  } else if (length(sampleYear) > 1){
    sampleYear <- paste0("('", paste0(sampleYear, collapse = "', '"),"')")
    wherestatement <- c(wherestatement,
                        paste0("extract(year from ssv.sample_site_visit_start_date) in ",
                               sampleYear))
  }

  if(length(sampleType) == 1){
    if(!is.na(sampleType)){
      sampleType <- paste0("('", sampleType,"')")
      wherestatement <- c(wherestatement,
                          paste0("ssv.sample_site_purpose_type_code in ",
                                 sampleType))
    }
  } else if(length(sampleType) > 1){
    sampleType <- paste0("('", paste0(sampleType, collapse = "', '"),"')")
    wherestatement <- c(wherestatement,
                        paste0("ssv.sample_site_purpose_type_code in ",
                               sampleType))
  }

  if(length(sampleProject) == 1){
    if(!is.na(sampleProject)){
      sampleProject <- paste0("('", sampleProject,"')")
      wherestatement <- c(wherestatement,
                          paste0("gsp.PROJECT_NAME in ",
                                 sampleProject))
    }
  } else if(length(sampleProject) > 1){
    sampleProject <- paste0("('", paste0(sampleProject, collapse = "', '"),"')")
    wherestatement <- c(wherestatement,
                        paste0("gsp.PROJECT_NAME in ",
                               sampleProject))
  }


  drv <- dbDriver("Oracle")
  connect_to_ismc <- getServer(databaseName = "ISMC",
                               envir = toupper(env))
  con <- dbConnect(drv, username = userName,
                   password = passWord,
                   dbname = connect_to_ismc)
  if(length(wherestatement) == 0){
    thedata <-
      dbGetQuery(con,
                 "select
                      ss.site_identifier,
                      ssv.visit_number,
                      gsp.project_name,
                      gsp.project_number,
                      ssv.sample_site_purpose_type_code,
                      ssv.sample_site_visit_start_date,
                      ssv.SAMPLE_SITE_VISIT_STATUS_CODE

                      from
                      app_ismc.ground_sample_project gsp

                      left join app_ismc.sample_site_visit ssv
                      on ssv.ground_sample_project_guic = gsp.ground_sample_project_guic

                      left join app_ismc.sample_site ss
                      on ss.sample_site_guic = ssv.sample_site_guic

                      left join app_ismc.psp_sample_site pspss
                      on pspss.sample_site_guic = ss.sample_site_guic


                      where
                      (pspss.PSP_SAMPLE_SITE_TYPE_CODE not in ('S')
                      or pspss.PSP_SAMPLE_SITE_TYPE_CODE is null)
                      order by
                      sample_site_visit_start_date, sample_site_purpose_type_code, project_name") %>%
      data.table
  } else {
    wherestatement <- paste0(wherestatement, collapse = " and ")
    thedata <-
      dbGetQuery(con,
                 paste0("select
                      gsp.project_name,
                      gsp.project_number,
                      ss.site_identifier,
                      ssv.visit_number,
                      ssv.sample_site_purpose_type_code,
                      ssv.sample_site_visit_start_date,
                      ssv.SAMPLE_SITE_VISIT_STATUS_CODE

                      from
                      app_ismc.ground_sample_project gsp

                      left join app_ismc.sample_site_visit ssv
                      on ssv.ground_sample_project_guic = gsp.ground_sample_project_guic

                      left join app_ismc.sample_site ss
                      on ss.sample_site_guic = ssv.sample_site_guic

                      left join app_ismc.psp_sample_site pspss
                      on pspss.sample_site_guic = ss.sample_site_guic

                      where
                      ", wherestatement,
                        "
                      and (pspss.PSP_SAMPLE_SITE_TYPE_CODE not in ('S')
                      or pspss.PSP_SAMPLE_SITE_TYPE_CODE is null)
                      order by
                      sample_site_visit_start_date, sample_site_purpose_type_code, project_name")) %>%
      data.table
  }
  dbDisconnect(con)
  return(thedata)
}
