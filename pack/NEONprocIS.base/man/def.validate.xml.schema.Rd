% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/def.validate.xml.schema.R
\name{def.validate.xml.schema}
\alias{def.validate.xml.schema}
\title{validate if the xml content is valid against the xml schema}
\usage{
def.validate.xml.schema(xmlIn, xmlSchemaIn, log = NULL)
}
\arguments{
\item{xmlIn}{Input xml to be validated}

\item{xmlSchemaIn}{Input xml schema to be validated against}
}
\value{
Boolean. TRUE if the input xml is valid against the schema. FALSE if not valid. \cr
}
\description{
Validate an input xml against the schema to check it is valid.
Returns TRUE if the xml is vaild . FALSE otherwise.
}
\examples{
NEONprocIS.base::def.validate.json.schema (jsonIn, jsonSchemaIn)
}
\references{
Currently none
}
\seealso{
\link[NEONprocIS.cal]{def.read.cal.xml}
}
\author{
Mija Choi \email{choim@battelleecology.org}
}
\keyword{Currently}
\keyword{none}
