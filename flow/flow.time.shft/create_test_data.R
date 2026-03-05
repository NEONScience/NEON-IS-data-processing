################################################################################
# Create synthetic fixture data for test-wrap-time-shft.R
#
# Run from: flow/tests/testthat/
#   Rscript testdata/create_time_shft_fixtures.R
#
# Creates three fixture directories under pfs/timeShft/:
#
#   pfs/timeShft/pluvio/2025/04/02/55221/
#     data/  pluvio_55221_2025-04-01.parquet  (1-min, 1440 rows)
#            pluvio_55221_2025-04-02.parquet  (1-min, 1440 rows)
#            pluvio_55221_2025-04-03.parquet  (1-min, 1440 rows)
#            manifest.txt
#     location/
#            location.json
#
#   pfs/timeShft/nopad/pluvio/2025/04/02/55222/
#     data/  pluvio_55222_2025-04-02.parquet  (center day only — pad incomplete)
#            manifest.txt
#
#   pfs/timeShft/nomanifest/pluvio/2025/04/02/55223/
#     data/  pluvio_55223_2025-04-01.parquet
#            pluvio_55223_2025-04-02.parquet
#            pluvio_55223_2025-04-03.parquet
#            (no manifest.txt)
################################################################################
setwd("~/GitHub/NEON-IS-data-processing/flow/tests/testthat")

# Helper: build 1-minute synthetic data for a single date
make_day_data <- function(date_str) {
  t_start <- as.POSIXct(paste(date_str, "00:00:00"), tz = "UTC")
  data.frame(
    readout_time = t_start + seq(0, 1440 - 1, by = 60),
    value        = round(runif(1440, 0, 100), 2)
  )
}

# Helper: write a parquet file
write_parq <- function(d, file) {
  NEONprocIS.base::def.wrte.parq(data = d, NameFile = file)
}

# ---- Fixture 1: full 3-day pad + manifest + location -------------------------
DirData1 <- 'pfs/timeShft/pluvio/2025/04/02/55221/data'
DirLoc1  <- 'pfs/timeShft/pluvio/2025/04/02/55221/location'
dir.create(DirData1, recursive = TRUE, showWarnings = FALSE)
dir.create(DirLoc1,  showWarnings = FALSE)

for (d in c('2025-04-01', '2025-04-02', '2025-04-03')) {
  write_parq(make_day_data(d),
             file.path(DirData1, paste0('pluvio_55221_', d, '.parquet')))
}
writeLines('', file.path(DirData1, 'manifest.txt'))
writeLines('{"location": "test_site"}', file.path(DirLoc1, 'location.json'))

cat('Created fixture 1 (full pad).\n')

# ---- Fixture 2: center day only, manifest present (pad incomplete) -----------
DirData2 <- 'pfs/timeShft/nopad/pluvio/2025/04/02/55222/data'
dir.create(DirData2, recursive = TRUE, showWarnings = FALSE)

write_parq(make_day_data('2025-04-02'),
           file.path(DirData2, 'pluvio_55222_2025-04-02.parquet'))
writeLines('', file.path(DirData2, 'manifest.txt'))

cat('Created fixture 2 (no pad).\n')

# ---- Fixture 3: full 3-day data, no manifest ---------------------------------
DirData3 <- 'pfs/timeShft/nomanifest/pluvio/2025/04/02/55223/data'
dir.create(DirData3, recursive = TRUE, showWarnings = FALSE)

for (d in c('2025-04-01', '2025-04-02', '2025-04-03')) {
  write_parq(make_day_data(d),
             file.path(DirData3, paste0('pluvio_55223_', d, '.parquet')))
}
# Intentionally no manifest.txt

cat('Created fixture 3 (no manifest).\n')
cat('Done. Run testthat::test_file("test-wrap-time-shft.R") to execute tests.\n')
