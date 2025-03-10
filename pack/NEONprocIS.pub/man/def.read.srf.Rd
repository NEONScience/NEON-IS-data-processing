% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/def.read.srf.R
\name{def.read.srf}
\alias{def.read.srf}
\title{Read Science Review Flags for NEON instrumented systems data products from JSON file to data frame}
\usage{
def.read.srf(NameFile = NULL, strJson = NULL, log = NULL)
}
\arguments{
\item{NameFile}{Filename (including relative or absolute path). Must be json format.}

\item{strJson}{character string of data in JSON format (as produced by rjson::toJSON()). Note that
only one of NameFile or strJson may be entered. If more than one are supplied, the first
valid input will be used.}

\item{log}{A logger object as produced by NEONprocIS.base::def.log.init to produce structured log
output. Defaults to NULL, in which the logger will be created and used within the function.}
}
\value{
A data frame with all science review flags contained in the json file.
}
\description{
Definition function. Read Science Review Flags information from json file and convert to data frame.
}
\examples{
Currently none
}
\references{
License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007
}
\seealso{
\link[NEONprocIS.qaqc]{def.read.thsh.qaqc.list}
}
\author{
Cove Sturtevant \email{csturtevant@battelleecology.org}
}
\keyword{Currently}
\keyword{none}
