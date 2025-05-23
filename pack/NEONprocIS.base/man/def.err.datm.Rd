% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/def.err.datm.R
\name{def.err.datm}
\alias{def.err.datm}
\title{Route datum errors to specified location}
\usage{
def.err.datm(
  err = NULL,
  call.stack = NULL,
  DirDatm,
  DirErrBase,
  RmvDatmOut = FALSE,
  DirOutBase = NULL,
  log = NULL
)
}
\arguments{
\item{err}{The error condition returned from a function call. For printing only. Defaults to NULL.}

\item{call.stack}{The call stack returned from e.g. base::sys.calls(). For printing only. Defaults to NULL.}

\item{DirDatm}{Character value. The input path to the datum, structured as follows: 
#/pfs/BASE_REPO/#, where # indicates any number of parent and child directories 
of any name, so long as they are not 'pfs'. Note that the path should terminate at 
the directory containing all the data and metadata (nested in further subdirectories 
as needed) considered to be one complete entity for processing. 
For example:
DirDatm = /scratch/pfs/proc_group/prt/2019/01/01/27134}

\item{DirErrBase}{Character value. The path that will replace the #/pfs/BASE_REPO portion of DirDatm}

\item{DirOutBase}{Character value. The path that will replace the #/pfs/BASE_REPO portion of 
DirIn when writing successful output for the datum.}

\item{log}{A logger object as produced by NEONprocIS.base::def.log.init to produce structured log
output in addition to standard R error messaging. Defaults to NULL, in which the logger will be
created and used within the function.}

\item{RmvDataOut}{Logical. TRUE to remove any partial output for the datum from the output repo. NOTE:
Removing partial output only works if the output datum path matches the typical structure where 
DirOutBase replaces the #/pfs/BASE_REPO portion of DirDatm, but otherwise is the same as DirDatm.
If this is not the case, set RmvDataOut to FALSE (which is the default).}
}
\value{
The action of creating the path structure to the datum within DirErrBase, having replaced 
the #/pfs/BASE_REPO portion of DirDatm with DirErrBase. If RmvDataOut is set to true, any partial
output for DirDatm will be removed. The output directory is the same as DirDatm but with the 
#/pfs/BASE_REPO portion of DirDatm replaced with DirOutBase.
}
\description{
Definition function. Route datum paths that errored to a specified error directory. 
The input path to the erroring datum will be created in the error directory.
Optionally, remove any partial output from the errored datum.
}
\examples{
# Not run:
DirDatm <- '/scratch/pfs/proc_group/prt/2019/01/01/27134'
DirErrBase <- '/scratch/pfs/proc_group_output/errored_datums'
RmvDatmOut <- TRUE
DirOutBase <- '/scratch/pfs/proc_group_output'
tryCatch(
    withCallingHandlers(
               stop('error!'),
               error=function(err) {
                       call.stack <- base::sys.calls() # is like a traceback within "withCallingHandlers"
                       def.err.datm(err=err,
                                    call.stack=call.stack,
                                    DirDatm=DirDatm,
                                    DirErrBase=DirErrBase,
                                    RmvDatmOut=RmvDatmOut,
                                    DirOutBase=DirOutBase)
                                    )
                       }
               ),
    error=function(err){print('I routed an error!')}
    )
    
}
\references{
License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007
}
\seealso{
tryCatch
}
\author{
Cove Sturtevant \email{csturtevant@battelleecology.org}
}
\keyword{Currently}
\keyword{none}
