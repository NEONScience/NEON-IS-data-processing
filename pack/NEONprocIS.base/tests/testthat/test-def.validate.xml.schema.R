#library(testthat)
#source("R/def.validate.xml.schema.R")

test_that("valid xml should return true",
          {
            xmlpath <- 'def.validate.xml.schema/calibration.xml'
            schemapath <- 'def.validate.xml.schema/calibration.xsd'
            returnData <- NEONprocIS.base::def.validate.xml.schema(xmlIn = xmlpath, xmlSchemaIn = schemapath)
            testthat::expect_true(returnData)
          }

)

test_that("invalid xml should return false",
          {
            xmlpath <- 'def.validate.xml.schema/invalid_calibration.xml'
            schemapath <- 'def.validate.xml.schema/calibration.xsd'
            returnData <- NEONprocIS.base::def.validate.xml.schema(xmlIn = xmlpath, xmlSchemaIn = schemapath)
            testthat::expect_false(returnData)
          }

)

