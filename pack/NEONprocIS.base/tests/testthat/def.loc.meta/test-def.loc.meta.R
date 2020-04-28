library(testthat)
source("R/def.loc.meta.R")


test_that("When No subdirectories, then each terminal directory is a datum",
          {
            nameFile <- "tests/testthat/def.loc.meta/test_input/pfs/prt_calibrated_location_group/prt/2019/01/01/16247"
          
            if (file.exists(file.path(nameFile))) {
              cat(
                "\n file exists====|\n"
              )
            
            }
           
            locationMetaData <- def.loc.meta(NameFile = nameFile)
            print("printing value of dirIn:")
            print(dirIn)
            
            testthat::expect_equal(c(dirOut1,dirOut2), dirIn)
          })

