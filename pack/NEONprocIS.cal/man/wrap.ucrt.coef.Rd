% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/wrap.ucrt.coef.R
\name{wrap.ucrt.coef}
\alias{wrap.ucrt.coef}
\title{Wrapper for compiling uncertainty coefficients for all variables and time ranges}
\usage{
wrap.ucrt.coef(calSlct, ucrtCoefFdas = NULL, mappNameVar = NULL, log = NULL)
}
\arguments{
\item{calSlct}{A named list of data frames, list element corresponding to the variable for which
uncertainty coefficients are to be compiled. The data frame in each list element holds 
information about the calibration files and time periods that apply to the variable, as returned 
from NEONprocIS.cal::def.cal.slct. See documentation for that function.}

\item{ucrtCoefFdas}{A data frame of FDAS uncertainty coefficients, as read by 
NEONprocIS.cal::def.read.ucrt.coef.fdas. Columns include:\cr
\code{Name} Character. Name of the coefficient.\cr
\code{Value} Character. Value of the coefficient.\cr
\code{.attrs} Character. Relevant attribute (i.e. units)\cr
Defaults to NULL, in which case no FDAS uncertainty coefficients will be combined with calibration
uncertanty coefs. Conversely, if this argument is present, the FDAS uncertainty coefficients contained
in this argument will be combined (and output) with calibration uncertianty coefs for all variables.}

\item{mappNameVar}{A data frame with in/out variable name mapping as produced by 
NEONprocIS.base::def.var.mapp.in.out. See documentation for that function. If input (default is
NULL), input variable names in the output data frames will be replaced by their corresponding 
output name.}

\item{log}{A logger object as produced by NEONprocIS.base::def.log.init to produce structured log
output. Defaults to NULL, in which the logger will be created and used within the function.}
}
\value{
A named list, each element corresponding to those in FuncUcrt$var and holding a data
frame of uncertainty coefficients and applicable time ranges.  \cr
}
\description{
Wrapper function. Compute individual measurement uncertainty for calibrated data at native
frequency (NEON L0' data).
}
\examples{
Currently none
}
\references{
Currently none
}
\seealso{
\link[NEONprocIS.cal]{def.cal.slct}

\link[NEONprocIS.cal]{def.read.cal.xml}

\link[NEONprocIS.base]{def.var.mapp.in.out}

\link[NEONprocIS.cal]{def.read.ucrt.coef.fdas}
}
\author{
Cove Sturtevant \email{csturtevant@battelleecology.org}
}
\keyword{Currently}
\keyword{none}
