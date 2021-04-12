sourceType <- 'exototalalgae'
dataType <- 'uncertainty_data' # folder e.g. 'data', 'flags', or 'uncertainty_data'
datePath <- '2020/01/01'
locId <- 'CFGLOC110700'
fileSuffix <- '_uncertaintyData' # include any underscores (e.g. '_flagsCal' or '_uncertaintyData'). Note: leave as '' for data directory.
  
data <- NEONprocIS.base::def.read.parq(NameFile=paste0('~/pfs/',
                                                       sourceType,
                                                       '_merge_data_by_location/',
                                                       sourceType,'/',
                                                       datePath,'/',
                                                       locId,'/',
                                                       dataType,'/',
                                                       sourceType,'_',
                                                       locId,'_',
                                                       gsub('/','-',datePath),
                                                       fileSuffix,
                                                       '.parquet'
                                                )
)

data <- data[FALSE,]

NEONprocIS.base::def.wrte.parq(data=data,NameFile=paste0('/scratch/pfs/empty_files/',
                                                         sourceType,'/',
                                                         dataType,'/',
                                                         sourceType,
                                                         '_location_year-month-day',
                                                         fileSuffix,
                                                         '.parquet'
                                                         )
)