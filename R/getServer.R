getServer <- function(databaseName, envir = "PROD"){
  if(databaseName == "GYS"){
    connect_string <-"(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)
    (HOST=nrkdb03.bcgov)(PORT=1521))
    (CONNECT_DATA=(SERVER=DEDICATED)(SERVICE_NAME=DBP07.NRS.BCGOV)))"
  } else if (databaseName == "VGIS"){
    connect_string <-"(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)
    (HOST=nrkdb01.bcgov)(PORT=1521))
    (CONNECT_DATA=(SERVER=DEDICATED)(SERVICE_NAME=ENVPROD1.NRS.BCGOV)))"
  } else if (databaseName == "ISMC"){
    if(envir == "INT"){
      connect_string <- "(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=TCP)
      (HOST=nrcdb01.bcgov)(PORT=1521)))
      (CONNECT_DATA=(SERVICE_NAME=ismcint.nrs.bcgov)))"
    } else if (envir == "TEST"){
      connect_string <- "(DESCRIPTION=(ADDRESS_LIST = (ADDRESS = (PROTOCOL = TCP)
      (HOST = nrcdb01.bcgov)(PORT = 1521)))
      (CONNECT_DATA = (SERVICE_NAME = ISMCTST.NRS.BCGOV)))"
    } else if (envir == "PROD"){
      connect_string <- "(DESCRIPTION=(ADDRESS_LIST = (ADDRESS = (PROTOCOL = TCP)
      (HOST = nrkdb01.bcgov)(PORT = 1521)))
      (CONNECT_DATA = (SERVICE_NAME = ismcprd.nrs.bcgov)))"
    }
  } else {
    stop("basebaseName must be specified among GYS, VGIS and ISMC")
  }
  return(connect_string)
}
