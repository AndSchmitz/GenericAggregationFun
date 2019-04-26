CalculateMonthlyAndAnnualAggs <- function(DataSubset,AggregationTimeSpan) {

  # R function to average over parallel measurements and aggregate measurements to a monthly or annual basis.
  # - The following aggregates are reported: Mean, Median, Min, Max, Sum and proportion of time covered by measurements per month or year.
  # - In a first step, parallel measurements are averaged on a daily basis. This allows averaging over
  # parallel measurements that only partially overlap in time, which is sometimes the case in the ICPF-DB.
  # - In a second step, the resulting daily values are aggregated to monthly or annual level.
  # I.e. in case of parallel measurements, aggregates (e.g. Max) are not calculated from original measurement values but from daily averages.

  require(Hmisc) #Provides number of days per month or year
  require(tidyverse) #data manipulation
    
  Sub <- DataSubset
  
  #Sanity checks
  ColsRequired <- c("ID","date_start","duration_days","value")
  if ( !all( ColsRequired %in% colnames(Sub) ) ) stop(paste("Input data frame for CalculateMonthlyAndAnnualAggs() requires the following columns:",paste(ColsRequired,collapse = ",")))
  Sub <- Sub[,ColsRequired]
  if ( class(Sub$date_start) != "Date" ) stop("Column date_start must be of class Date.")
  SubID <- unique(Sub$ID)
  if ( (length(SubID) != 1) ) stop("ID must be unique among data send to CalculateMonthlyAndAnnualAggs()")
  if ( !(AggregationTimeSpan %in% c("monthly","annual")) ) stop("AggregationTimeSpan must be either monthly or annual")
  if ( any(Sub$duration_days < 1) ) stop("Values in column duration_days must be equal or larger than 1.")
  if ( any(is.na(Sub$duration_days)) ) stop("Values in column duration_days must not be NA")
  
  #Basic idea: Repeat the each row as many times as the corresponding period has duration_days.
  #This yields data on a daily level. Perform monthly or annual aggregation on this dataset.
  #Benefits:
  # 1. You can arbitrarily aggregate over time (years, month, bi-weekly).
  # 2. Priods reaching over new years day (in case of annual aggregation)
  #or over the beginning of a month (in case of monthly aggregation) are not problematic.
  # 3. Aggregation of parallel measurements from only partially overlapping periods
  #(e.g. slightly different date_start,date_end) is not problematic.
  
  #Stretching data to a daily level
  #Create repetitions of each data set. The number of times of the repetition corresponds to the period duration.
  #Continuous assignment of days since 1900 to repetitions of each substance-period, from date_start to date_end
  Sub$DataSetID <- 1:nrow(Sub)
  SubDaily <- Sub[rep(row.names(Sub), Sub$duration_days), ]
  
  #Now assign a daily date to each row
  SubDaily$FirstDayOfDataSetID <- c(T,diff(SubDaily$DataSetID) != 0)
  SubDaily$i <- 1:nrow(SubDaily)
  SubDaily$Correction <- -rep(SubDaily$i[SubDaily$FirstDayOfDataSetID],SubDaily$duration_days[SubDaily$FirstDayOfDataSetID])
  SubDaily$RunningDayInPeriod <- SubDaily$i + SubDaily$Correction
  SubDaily$date_start_d1900 <- as.numeric(difftime(time1=SubDaily$date_start,time2=as.Date("1900-01-01"),units="days"))
  SubDaily$d_1900 <- SubDaily$date_start_d1900 + SubDaily$RunningDayInPeriod
  
  #Create daily averages - avg over parallel measurements
  SubDailyAvg <- aggregate(value ~ d_1900,data=SubDaily,FUN=mean)
  
  #Create monthly / annual aggregates: averages and sums
  SubDailyAvg$date <- as.Date("1900-01-01") + SubDailyAvg$d_1900
  SubDailyAvg$year <- as.numeric(strftime(x=SubDailyAvg$date,format="%Y"))
  SubDailyAvg$month <- as.numeric(strftime(x=SubDailyAvg$date,format="%m"))
  
  if ( AggregationTimeSpan == "monthly" ) {
    Avg <- aggregate(value ~ year + month,data=SubDailyAvg,FUN=mean)
    Avg <- rename(Avg,"MonthlyAvg" = "value")
    Median <- aggregate(value ~ year + month,data=SubDailyAvg,FUN=median)
    Median <- rename(Median,"MonthlyMedian" = "value")
    Sum <- aggregate(value ~ year + month,data=SubDailyAvg,FUN=sum)
    Sum <- rename(Sum,"MonthlySum" = "value")
    Min <- aggregate(value ~ year + month,data=SubDailyAvg,FUN=min)
    Min <- rename(Min,"MonthlyMin" = "value")
    Max <- aggregate(value ~ year + month,data=SubDailyAvg,FUN=max)
    Max <- rename(Max,"MonthlyMax" = "value")    
    TempCover <- aggregate(value ~ year + month,data=SubDailyAvg,FUN=length)
    TempCover <- rename(TempCover,"nDaysWithData" = "value")
    TempCover$nDaysOfMonth <- monthDays(as.Date(paste(TempCover$year,TempCover$month,"15",sep="-")))
    TempCover$PropTempCover <- TempCover$nDaysWithData / TempCover$nDaysOfMonth
    TempCover <- TempCover[,c("year","month","PropTempCover")]
  } else if ( AggregationTimeSpan == "annual" ) {
    Avg <- aggregate(value ~ year,data=SubDailyAvg,FUN=mean)
    Avg <- rename(Avg,"AnnualAvg" = "value")
    Median <- aggregate(value ~ year,data=SubDailyAvg,FUN=median)
    Median <- rename(Median,"AnnualMedian" = "value")    
    Sum <- aggregate(value ~ year,data=SubDailyAvg,FUN=sum)
    Sum <- rename(Sum,"AnnualSum" = "value")
    Min <- aggregate(value ~ year,data=SubDailyAvg,FUN=min)
    Min <- rename(Min,"AnnualMin" = "value")
    Max <- aggregate(value ~ year,data=SubDailyAvg,FUN=max)
    Max <- rename(Max,"AnnualMax" = "value")        
    TempCover <- aggregate(value ~ year,data=SubDailyAvg,FUN=length)
    TempCover <- rename(TempCover,"nDaysWithData" = "value")
    TempCover$nDaysOfYear <- yearDays(as.Date(paste(TempCover$year,"06","15",sep="-")))
    TempCover$PropTempCover <- TempCover$nDaysWithData / TempCover$nDaysOfYear
    TempCover <- TempCover[,c("year","PropTempCover")]
  }
  
  #Merge aggregates
  Aggs <- merge(Avg,Median)
  Aggs <- merge(Aggs,Sum)
  Aggs <- merge(Aggs,Min)
  Aggs <- merge(Aggs,Max)
  Aggs <- merge(Aggs,TempCover)
  Aggs$ID <- SubID
  
  return(Aggs)
}




