library(testthat)
source("R/def.dir.crea.R")

test_that("Data directory created without subpath",
          {
            dirBgn <- "tests/testthat/def.dir.crea/test_output"
            dirSub <- "dirCreaTest"
            def.dir.crea(DirBgn = dirBgn, DirSub =dirSub)
            testthat::expect_true(dir.exists("tests/testthat/def.dir.crea/test_output/dirCreaTest"))
            
          })







