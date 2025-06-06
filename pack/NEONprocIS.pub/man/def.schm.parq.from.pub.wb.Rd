% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/def.schm.parq.from.pub.wb.R
\name{def.schm.parq.from.pub.wb}
\alias{def.schm.parq.from.pub.wb}
\title{Create Parquet schema from publication workbook}
\usage{
def.schm.parq.from.pub.wb(
  NameFile = NULL,
  pubWb = NULL,
  TablPub = NULL,
  log = NULL
)
}
\arguments{
\item{NameFile}{String. Optional (either one of NameFile or PubWb must be entered). 
Name (including relative or absolute path) of publication workbook file.}

\item{pubWb}{Data frame. Optional (either one of NameFile or PubWb must be entered). A data frame
of the pub workbook, as read in by def.read.pub.wb.}

\item{TablPub}{Character vector. The table(s) in the pub workbook(s) to produce. By default all of them 
are produced.}

\item{log}{A logger object as produced by NEONprocIS.base::def.log.init to produce structured log
output. Defaults to NULL, in which the logger will be created and used within the function.}
}
\value{
An Apache Arrow Schema object containing a parquet schema representing the table(s) in the 
publication workbook.
}
\description{
Definition function. Create a arrow schema object from a publication workbook. The fields in the
schema for each table will be crafted in the order the terms are found in the pub workbook, 
with any duplicates removed. Note that the rank column in the pub workbook is not heeded in 
order to avoid issues when multiple pub workbooks are combined.
}
\examples{
Currently none
}
\references{
License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007
}
\seealso{
\link[arrow]{data-type}

\link[NEONprocIS.pub]{def.read.pub.wb}
}
\author{
Cove Sturtevant \email{csturtevant@battelleecology.org}
}
\keyword{Currently}
\keyword{none}
