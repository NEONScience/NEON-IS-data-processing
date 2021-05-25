source("~/R/omnibus-tools/omnibus-tools/pipelines/r_utility_tools/def.download.data.R")
source("~/R/omnibus-tools/omnibus-tools/pipelines/r_utility_tools/def.data.comp.R")
source("~/R/L0-tracking-glitt/pkg/alerts/R/def.set.s3.env.R")
# CFGLOC110702 - BARC
# CFGLOC110820 - SUGG
# Must set wd to location of ravro.so
setwd("~/R/NEON-IS-data-processing-glitt/pack/NEONprocIS.base/")

pachydermrepo <- "tempSpecificDepthLakes_level1_reshape"
subDir <- "tchain/2019/01/10/CFGLOC110702/level1_reshape/"
site <- "BARC"
dpid <- "DP1.20264.001"
temporalindex <- "030"
startdate <- "2019-01-10"
enddate <- startdate
ravroPath <- "/home/NEON/glitt/R/NEON-IS-data-processing-glitt/pack/NEONprocIS.base/ravro.so"
plotnumber <- NULL
namedLocationName <- "CFGLOC110702"
outputfilepath <- "/home/NEON/glitt/pfs/compareToPortal/tsdl/" # DO NOT ENTER RELATIVE PATH, i.e. ~/ is bad!
RmvDataDnld <- TRUE
log <- NULL

# ------------------------------- RUN SCRIPT -------------------------------- #

# Set the S3 bucket read creds: Note that this requires adding read creds to your ~/.profile file.
def.set.s3.env(bucket = "prod-is-transition-output",
               AWS_ACCESS_KEY_ID = 'prod-is-reader',
               AWS_DEFAULT_REGION = "s3.data",
               AWS_S3_ENDPOINT = "neonscience.org",
               scrtTitl = "AWS_PROD_TRANS_SECRET=")

def.download.data(pachydermrepo = pachydermrepo,
                  subDir = subDir,
                  site = site,
                  dpid = dpid,
                  temporalindex = temporalindex,
                  startdate = startdate,
                  enddate = enddate,
                  ravroPath = ravroPath,
                  plotnumber = plotnumber,
                  namedLocationName = namedLocationName,
                  outputfilepath = outputfilepath,
                  RmvDataDnld = RmvDataDnld,
                  log = log)
print("Inspect the following: ")
list.files(outputfilepath,recursive = TRUE)


