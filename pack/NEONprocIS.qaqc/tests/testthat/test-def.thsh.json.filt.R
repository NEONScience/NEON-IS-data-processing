#library(testthat)
#source("R/def.thsh.json.filt.R")

test_that("NameFile is not null and both term and ctxt are null",
          {
            nameFileIn <- "def.read.thsh.qaqc.df/thresholds.json"
            nameFileOut <- "thresholds-out.json"
            rpt <- NEONprocIS.qaqc::def.thsh.json.filt(NameFile = nameFileIn, NameFileOut = nameFileOut)
            expect_true (file.exists(nameFileOut))
            thsh <- rjson::fromJSON(file=nameFileOut,simplify=TRUE)$thresholds
            testthat::expect_true(length(thsh) == 4)
            if (file.exists(nameFileOut)) {
              file.remove(nameFileOut)
            }

          }

)
test_that("NameFile and strJson are both null, stop processing",
          {
            nameFileOut <- "thresholds-out.json"
            rpt <- try(NEONprocIS.qaqc::def.thsh.json.filt(NameFileOut = nameFileOut), silent = TRUE)
            testthat::expect_true((class(rpt)[1] == "try-error"))

          }

)
test_that("NameFile is not null and ctxt is not null",
          {
            nameFileIn <- "def.read.thsh.qaqc.df/thresholds.json"
            nameFileOut <- "thresholds-out.json"
            ctxt <- c("soil", "water")
            rpt <- NEONprocIS.qaqc::def.thsh.json.filt(NameFile = nameFileIn, NameFileOut = nameFileOut, Ctxt = ctxt)
            expect_true (file.exists(nameFileOut))
            thsh <- rjson::fromJSON(file=nameFileOut,simplify=TRUE)$thresholds
            testthat::expect_true(length(thsh) == 1)
            if (file.exists(nameFileOut)) {
              file.remove(nameFileOut)
            }
          }
)
test_that("NameFile and term are not null and ctxt is null",
          {
            nameFileIn <- "def.read.thsh.qaqc.df/thresholds.json"
            nameFileOut <- "thresholds-out.json"
            term <- "TFPrecipBulk"
            rpt <- NEONprocIS.qaqc::def.thsh.json.filt(NameFile = nameFileIn, NameFileOut = nameFileOut, Term = term)
            expect_true (file.exists(nameFileOut))
            thsh <- rjson::fromJSON(file=nameFileOut,simplify=TRUE)$thresholds
            testthat::expect_true(length(thsh) == 1)
            if (file.exists(nameFileOut)) {
              file.remove(nameFileOut)
            }
          }
)
test_that("NameFile is not null and ctxt is partial",
          {
            nameFileIn <- "def.read.thsh.qaqc.df/thresholds.json"
            nameFileOut <- "thresholds-out.json"
            ctxt <- c("soil")
            rpt <- NEONprocIS.qaqc::def.thsh.json.filt(NameFile = nameFileIn, NameFileOut = nameFileOut, Ctxt = ctxt)
            expect_true (file.exists(nameFileOut))
            thsh <- rjson::fromJSON(file=nameFileOut,simplify=TRUE)$thresholds
            testthat::expect_true(length(thsh) == 1)
            if (file.exists(nameFileOut)) {
              file.remove(nameFileOut)
            }
          }
)
test_that("NameFile is not null and both term and ctxt are not null and not exists in the same threshold",
          {
            nameFileIn <- "def.read.thsh.qaqc.df/thresholds.json"
            nameFileOut <- "thresholds-out.json"
            ctxt <- c("soil")
            term <- "rawVSWC7"
            rpt <- NEONprocIS.qaqc::def.thsh.json.filt(NameFile = nameFileIn, NameFileOut = nameFileOut, Term = term, Ctxt = ctxt)
            expect_true (file.exists(nameFileOut))
            thsh <- rjson::fromJSON(file=nameFileOut,simplify=TRUE)$thresholds
            testthat::expect_true(length(thsh) == 0)
            if (file.exists(nameFileOut)) {
              file.remove(nameFileOut)
            }
          }
)
test_that("NameFile is not null and both term and ctxt are not null and exists in the same threshold",
          {
            nameFileIn <- "def.read.thsh.qaqc.df/thresholds.json"
            nameFileOut <- "thresholds-out.json"
            ctxt <- c("water")
            term <- "temp"
            rpt <- NEONprocIS.qaqc::def.thsh.json.filt(NameFile = nameFileIn, NameFileOut = nameFileOut, Term = term, Ctxt = ctxt)
            expect_true (file.exists(nameFileOut))
            thsh <- rjson::fromJSON(file=nameFileOut,simplify=TRUE)$thresholds
            testthat::expect_true(length(thsh) == 1)
            if (file.exists(nameFileOut)) {
              file.remove(nameFileOut)
            }
          }
)

test_that("NameFile is not null and NameFileOut is NULL",
          {
            nameFileIn <- "def.read.thsh.qaqc.df/thresholds.json"
            nameFileOut <- "thresholds-out.json"
            ctxt <- c("water")
            term <- "temp"
            rpt <- NEONprocIS.qaqc::def.thsh.json.filt(NameFile = nameFileIn, Term = term, Ctxt = ctxt)
            expect_false (file.exists(nameFileOut))
            expect_true(class(rpt) == "list")
            if (file.exists(nameFileOut)) {
              file.remove(nameFileOut)
            }
          }
)