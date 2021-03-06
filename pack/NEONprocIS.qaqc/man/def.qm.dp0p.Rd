% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/def.qm.dp0p.R
\name{def.qm.dp0p}
\alias{def.qm.dp0p}
\title{Compute "instantaneous" alpha, beta, and final quality flags}
\usage{
def.qm.dp0p(qf, Para = NULL, log = NULL)
}
\arguments{
\item{qf}{Data frame of named quality flags (values of -1,0,1)}

\item{Para}{(optional) A named list of:\cr
\code{qfAlph} A character vector of the names of quality flags in \code{qf} that are to be used
to compute AlphaQF. If any of these flags have a value of 1 for a given record, AlphaQF will be
1 for that record. May be NULL (or not in the list), in which case all flags found in \code{qf}
will be used to compute AlphaQF.\cr
\code{qfBeta} A character vector of the names of quality flags in \code{qf} that are to be used
to compute BetaQF. If any of these flags have a value of -1 for a given record, BetaQF will be
1 for that record. May be NULL (or not in the list), in which case all flags found in \code{qf}
will be used to compute BetaQF. Note that this action may be modified by the \code{qfBetaIgnr}
list element below\cr
\code{qfBetaIgnr} A character vector of the names of quality flags in \code{qf} that, if any of
their values equals 1 for a particular record, the betaQF flag for that record is automatically
set to 0 (ignores the values of all other flags). May be NULL, (or not in the list), in which
case this argument will be ignored.
Note that the entire Para argument defaults to NULL, which will follow the default actions
described above.}

\item{log}{A logger object as produced by NEONprocIS.base::def.log.init to produce structured log
output. Defaults to NULL, in which the logger will be created and used within the function.}
}
\value{
A data frame of the alpha, beta, and final quality flags
}
\description{
Definition function. Aggregate quality flags to produce alpha, beta, and final quality flags
for each L0' (instantaneous) record. The alpha flag is 1 when any of a set of selected
flags have a value of 1 (fail). The beta flag is 1 when any of a set of selected
flags cannot be evaluated (have a value of -1). If either the alpha flag or beta flag are raised,
the final quality is raised (value of 1).
}
\examples{
qf <- data.frame(QF1=c(1,-1,1,0,-1),QF2=c(-1,1,0,0,0),stringsAsFactors=FALSE)
Para <- list(qfAlph=c('QF1','QF2'),qfBeta=c('QF1','QF2'),qfBetaIgnr='QF2')
def.qm.dp0p(qf=qf,Para=Para)
}
\references{
License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007
}
\seealso{
Currently none
}
\author{
Cove Sturtevant \email{csturtevant@battelleecology.org}
}
\keyword{Currently}
\keyword{none}
