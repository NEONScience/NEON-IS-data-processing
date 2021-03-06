test_that("Data directory multiple input paths",
          {
            dirBgn <- "def.dir.in/test_input/pfs/27134"
            subDir <- c("data", "calibration")
            dirIn <-  NEONprocIS.base::def.dir.in(DirBgn = dirBgn, nameDirSub =subDir)
            print("printing value of dirIn in test 1:")
            print(dirIn)
            dirOut1 <- "def.dir.in/test_input/pfs/27134/2019/01/01"
            testthat::expect_equal(c(dirOut1), dirIn)

          })


test_that("Input directory path is returned only if all the subDir are in the same directory",
          {
            dirBgn <- "def.dir.in/test_input/pfs/27135"
            subDir <- c("data", "calibration")
            dirIn <-  NEONprocIS.base::def.dir.in(DirBgn = dirBgn, nameDirSub =subDir)
            print("printing value of dirIn in test 2:")
            print(dirIn)
            testthat::expect_equal(0, length(dirIn))
          })





test_that("When No subdirectories, then each terminal directory is a datum",
          {
            dirBgn <- "def.dir.in/test_input/pfs/27135"
            subDir <- NULL
            dirIn <-  NEONprocIS.base::def.dir.in(DirBgn = dirBgn, nameDirSub =subDir)
            print("printing value of dirIn:")
            print(dirIn)
            dirOut1 <- "def.dir.in/test_input/pfs/27135/outer/data"
            dirOut2 <- "def.dir.in/test_input/pfs/27135/outer/inner/calibration"
            testthat::expect_equal(c(dirOut1,dirOut2), dirIn)
          })

