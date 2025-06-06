% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/def.thsh.slct.R
\name{def.thsh.slct}
\alias{def.thsh.slct}
\title{Determine set of applicable QA/QC thresholds for date, location, term, and context}
\usage{
def.thsh.slct(
  thsh,
  Time,
  Term,
  Ctxt = NULL,
  Site = NULL,
  NameLoc = NULL,
  RptThsh = TRUE,
  log = NULL
)
}
\arguments{
\item{thsh}{List of thresholds, as returned from NEONprocIS.qaqc::def.read.thsh.qaqc.list}

\item{Time}{POSIXct value of the day to select thresholds for (assumes time resolution 
for thresholds is 1 day). Time should be at 00:00:00 GMT}

\item{Term}{Character value. The term for which to select thresholds for.}

\item{Ctxt}{Character vector (optional) . The contexts for which to select thresholds for. Treated 
as an AND with \code{Term}, meaning that the thresholds are selected which match both the Term 
and all contexts. Defaults to NULL, in which case the criteria for threshold selection is limited
 to the term.}

\item{Site}{Character value. The NEON site code. (e.g. HARV). If NULL (default), the REALM 
thresholds will be selected.}

\item{NameLoc}{Character value. The specific named location of the sensor. If NULL (default), 
the REALM thresholds will be selected.}

\item{RptThsh}{Logical value. If TRUE, the filtered list of thresholds is output. If FALSE, the
indices of the selected thresholds in the input list is returned. Defaults to TRUE.}

\item{log}{A logger object as produced by NEONprocIS.base::def.log.init to produce structured log
output in addition to standard R error messaging. Defaults to NULL, in which the logger will be
created and used within the function.}
}
\value{
If the RptThsh argument is TRUE, the filtered (selected) list of thresholds is output 
in the same format as input \code{thsh}. If RptThsh is false, the indices of the selected 
thresholds in the input list \code{thsh} is returned.
}
\description{
Definition function. Given a json file of thresholds, return those that are applicable to the
date, term (variable), and context (all properties of the thresholds). The choice of 
constraint/threshold to use is determined by moving up the following hierarchy 
from finer to coarser constraints until one applies. Thus, the finest applicable level of constraint 
is chosen. Threshold selection order is as follows (1 being the finest possible contraint): 
6. Realm, annual
5. Realm, seasonal
4. Site-specific, annual
3. Site-specific, seasonal
2. Sensor-spefific, annual
1. Sensor-specific, seasonal
}
\examples{
Currently none
}
\references{
License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007
}
\seealso{
\link[NEONprocIS.qaqc]{def.read.thsh.qaqc.df}

\link[NEONprocIS.qaqc]{def.read.thsh.qaqc.list}
}
\author{
Cove Sturtevant \email{csturtevant@battelleecology.org}
}
\keyword{QA/QC}
\keyword{QA/QC,}
\keyword{assurance,}
\keyword{control,}
\keyword{quality}
\keyword{test}
