% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/loadGYS.R
\name{loadGYS}
\alias{loadGYS}
\title{Load the data from GYS Oracle database}
\usage{
loadGYS(userName, passWord, savePath = file.path("."))
}
\arguments{
\item{userName}{character, Specifies a valid user name in GYS Oracle database.}

\item{passWord}{character, Specifies the password to the user name.}

\item{savePath}{character, Specifies the path to save your outputs, you do not need to
specify if \code{saveThem} is turned off. If missing, the current working
directory will be choosed.}
}
\value{
no value returned
}
\description{
This function is to load the natural growth PSP data from GYS Oracle database. This function
is adapted from the first part of \code{cv_gys.sas}.
}
\author{
Yong Luo
}
