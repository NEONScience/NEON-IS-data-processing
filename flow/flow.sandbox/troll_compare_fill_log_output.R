
year <- '2022'
month <- '07'
day <- '01'

DirLog <- paste0('/scratch/pfs/troll_logjam_assign_clean_files/leveltroll500/',year,'/',month,'/',day,'/')
DirTrino <- paste0('/scratch/pfs/troll_data_source_trino/leveltroll500/',year,'/',month,'/',day,'/')
DirFill <- paste0('/scratch/pfs/troll_fill_log_files/leveltroll500/',year,'/',month,'/',day,'/')
uid_shifted <- c()

uid_list <- list.dirs(DirFill,full.names = FALSE, recursive = FALSE)
#uid_list <-  '43700' #'21131' # (time offset example) '43700' #(1st pt) #'43692' (unch) 

for (uid in uid_list){
  try({
    print(uid)
    dataLogFile <- NEONprocIS.base::def.read.parq(NameFile=paste0(DirLog,uid,'/data/leveltroll500_',uid,'_',year,'-',month,'-',day,'_log.parquet'))
    dataTrino <- NEONprocIS.base::def.read.parq(NameFile=paste0(DirTrino,uid,'/data/leveltroll500_',uid,'_',year,'-',month,'-',day,'.parquet'))
    dataFill <- NEONprocIS.base::def.read.parq(NameFile=paste0(DirFill,uid,'/data/leveltroll500_',uid,'_',year,'-',month,'-',day,'.parquet'))
  
    shifted <- any(is.na(dataFill[c(1),'pressure_data_quality']))
    if(shifted == TRUE){
      message('Shifted') 
      uid_shifted <- c(uid_shifted,uid)
    }
  
  },silent=TRUE)
}

message('Shifted files:',paste0(uid_shifted,collapse=','))

# Plot the shifted ones
for (uid in uid_shifted){
  print(paste0('Plotting ',uid))
  
  dataLogFile <- NEONprocIS.base::def.read.parq(NameFile=paste0(DirLog,uid,'/data/leveltroll500_',uid,'_',year,'-',month,'-',day,'_log.parquet'))
  dataTrino <- NEONprocIS.base::def.read.parq(NameFile=paste0(DirTrino,uid,'/data/leveltroll500_',uid,'_',year,'-',month,'-',day,'.parquet'))
  dataFill <- NEONprocIS.base::def.read.parq(NameFile=paste0(DirFill,uid,'/data/leveltroll500_',uid,'_',year,'-',month,'-',day,'.parquet'))
  
  dataPlot <- data.frame(readout_time=c(dataTrino$readout_time,
                                        dataLogFile$readout_time,
                                        dataFill$readout_time),
                         pressure=c(dataTrino$pressure,
                                    as.numeric(dataLogFile$pressure),
                                    dataFill$pressure),
                         file=c(rep('trino',nrow(dataTrino)),
                                rep('Log',nrow(dataLogFile)),
                                rep('filled',nrow(dataFill))))
  
  p <- plot_ly(data=dataPlot,x=~readout_time,y=~pressure,color=~file,mode='lines',type='scatter') %>% 
    plotly::layout(title = uid, showlegend=TRUE)
  print(p)
}
