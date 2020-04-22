library(testthat)
source("R/def.dir.in.R")

# test_that("Data directory multiple input paths",
#           {
#             dirBgn <- "tests/testthat/def.dir.in/test_input/pfs"
#             subDir <- c("data", "calibration")
#             dirIn <- def.dir.in(DirBgn = dirBgn, nameDirSub =subDir)
#             print("printing value of dirIn:")
#             print(dirIn)
#             dirOut1 <- "tests/testthat/def.dir.in/test_input/pfs/27134/2019/01/01"
#             dirOut2 <- "tests/testthat/def.dir.in/test_input/pfs/27134/2019/01/02/outer"
#             testthat::expect_equal(c(dirOut1,dirOut2), dirIn)
#          
#           })


test_that("Input directory path is returned only if all the subDir are in the same directory",
          {
            dirBgn <- "tests/testthat/def.dir.in/test_input/pfs/27135"
            subDir <- c("data", "calibration")
            dirIn <- def.dir.in(DirBgn = dirBgn, nameDirSub =subDir)
            print("printing value of dirIn:")
            print(dirIn)
            testthat::expect_equal(0, length(dirIn))
          })







