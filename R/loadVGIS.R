#' Load the data from VGIS Oracle database
#'
#'
#' @description This function is to read the data from VGIS Oracle database
#'
#' @param userName character, Specifies a valid user name in VGIS Oracle database.
#' @param passWord character, Specifies the password to the user name.
#' @param saveThem logical, Specifies whether the loaded data should be saved or returned.
#'                 The default value is FALSE, which means the function will not save files
#'                 for you.
#' @param savePath character, Specifies the path that directs to the VRI original data soruce, i.e.,
#'                                  \code{//Mayhem/GIS_TIB/RDW/RDW_Data2/Work_Areas/VRI_ASCII_PROD/vri_sa}.
#'
#' @return no files
#' @export
#'
#' @rdname loadVGIS
#' @author Yong Luo
loadVGIS <- function(userName, passWord,
                     saveThem = FALSE, savePath = "."){
  loadVGISSample(userName = userName,
                 passWord = passWord,
                 saveThem = saveThem,
                 savePath = savePath)
  loadVGISCrew(userName = userName,
               passWord = passWord,
               saveThem = saveThem,
               savePath = savePath)
  loadVGISSampleAccess(userName = userName,
                       passWord = passWord,
                       saveThem = saveThem,
                       savePath = savePath)
  loadVGISPlot(userName = userName,
               passWord = passWord,
               saveThem = saveThem,
               savePath = savePath)
  loadVGISRange(userName = userName,
                passWord = passWord,
                saveThem = saveThem,
                savePath = savePath)
  loadVGISCWD(userName = userName,
              passWord = passWord,
              saveThem = saveThem,
              savePath = savePath)
  loadVGISTreeC(userName = userName,
                passWord = passWord,
                saveThem = saveThem,
                savePath = savePath)
  loadVGISLossIndicator(userName = userName,
                        passWord = passWord,
                        saveThem = saveThem,
                        savePath = savePath)
  loadVGISSiteTree(userName = userName,
                   passWord = passWord,
                   saveThem = saveThem,
                   savePath = savePath)
  loadVGISStumpSTree(userName = userName,
                     passWord = passWord,
                     saveThem = saveThem,
                     savePath = savePath)
  loadVGISTreeI(userName = userName,
                passWord = passWord,
                saveThem = saveThem,
                savePath = savePath)
  loadVGISEcology(userName = userName,
                  passWord = passWord,
                  saveThem = saveThem,
                  savePath = savePath)
  loadVGISVeg(userName = userName,
              passWord = passWord,
              saveThem = saveThem,
              savePath = savePath)
  loadVGISSuccession(userName = userName,
                     passWord = passWord,
                     saveThem = saveThem,
                     savePath = savePath)
  loadVGISNotes(userName = userName,
                passWord = passWord,
                saveThem = saveThem,
                savePath = savePath)
  loadVGISPhoto(userName = userName,
                passWord = passWord,
                saveThem = saveThem,
                savePath = savePath)
}
