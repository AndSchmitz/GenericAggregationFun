TemporalAggregation <- function(DataToAggregate,AggregationPeriods,FixDateStartEndOverlap=T) {

  # R function to average over parallel measurements and aggregate measurements to a monthly or annual basis.
  # - The following aggregates are reported: Mean, Median, Min, Max, Sum and proportion of time covered by measurements per month or year.
  # - In a first step, parallel measurements are averaged on a daily basis. This allows averaging over
  # parallel measurements that only partially overlap in time, which is sometimes the case in the ICPF-DB.
  # - In a second step, the resulting daily values are aggregated to monthly or annual level.
  # I.e. in case of parallel measurements, aggregates (e.g. Max) are not calculated from original measurement values but from daily averages.

  require(parallel)
  require(lubridate)

  
  #Sanity checks
  ColsRequiredData <- c("ID","date_start","date_end","value")
  if ( !all( ColsRequiredData %in% colnames(DataToAggregate) ) ) stop(paste("Input data frame for TemporalAggregation() requires the following columns:",paste(ColsRequiredData,collapse = ",")))
  DataToAggregate <- DataToAggregate[,ColsRequiredData]
  if ( class(DataToAggregate$date_start) != "Date" ) stop("Column date_start must be of class Date.")
  if ( class(DataToAggregate$date_end) != "Date" ) stop("Column date_end must be of class Date.")
  if ( any(is.na(DataToAggregate$date_start)) ) stop("Values in column date_start must not be NA")
  if ( any(is.na(DataToAggregate$date_end)) ) stop("Values in column date_end must not be NA")
  #Do not allow cases with negative period duration
  nCasesNegDuration =  sum( DataToAggregate$date_start > DataToAggregate$date_end )
  if ( nCasesNegDuration > 0 ) {
    stop(paste(nCasesNegDuration,"datasets found where date_start > date_end. This is currently not supported by function TemporalAggregation()."))
  }

  DataToAggregate <- DataToAggregate %>%
    mutate(
      #Calculate period duration. It is always assumed that measurements started
      #on date_start at 00:00 and ended on date_end at 23:59, i.e. that both dates
      #are completely covered by measurements. Thus, +1:
      duration_days = as.numeric(difftime(date_end,date_start,units = "days")) + 1
    )
  
  #Determine whether AggregationPeriods is a data frame with specified start and end dates for aggregation or
  #whether it is a string with allowed values "monthly" or "annual"
  ColsRequiredAggregationPeriods <- c("ID","date_aggregation_start","date_aggregation_end")
  if ( any(class(AggregationPeriods) == "data.frame") ) {
    if ( !all( ColsRequiredAggregationPeriods %in% colnames(AggregationPeriods) ) ) stop(paste("AggregationPeriods data frame for TemporalAggregation() requires the following columns:",paste(ColsRequiredAggregationPeriods,collapse = ",")))
    if ( nrow(AggregationPeriods) == 0 ) stop("AggregationPeriods must be a data frame with > 0 rows.")
    IDsMissInData <- AggregationPeriods$ID[!(AggregationPeriods$ID %in% DataToAggregate$ID)]
    if ( length(IDsMissInData) > 0 ) {
      stop(paste("The following IDs are requested for aggregation according to AggregationPeriods data frame but are not present in DataToAggregate data frame:",paste(IDsMissInData,collapse=",")))
    }
  } else if ( all(class(AggregationPeriods) != "character")  ) {
    stop("AggregationPeriods must either be a data frame or a character string.")
  } else {
    if ( !(AggregationPeriods %in% c("monthly","annual")) ) stop("If AggregationPeriods is character it must be either 'monthly' or 'annual'")
    #Manually create an 'AggregationPeriods' data frame from 'monthly' or 'annual' specifications
    #Identify all unique years or months per ID for which data is available and schedule
    #aggregation for these periods
    tmp <- DataToAggregate %>%
      mutate(
        date_end = date_start + duration_days,
        Month_start = as.Date(paste0(strftime(x=date_start,format = "%Y-%m"),"-01")),
        Month_end = as.Date(paste0(strftime(x=date_end,format = "%Y-%m"),"-01"))
      ) %>%
      select(
        ID,Month_start,Month_end
      ) %>%
      gather(key=StartEnd,value=MonthInData,-ID) %>%
      select(-StartEnd) %>%
      distinct()
      
    #For annual aggregation
    if ( AggregationPeriods == "annual" ) {
      AggregationPeriodsTmp <- tmp %>%
        mutate(
          Year = strftime(x=MonthInData,format="%Y"),
          date_aggregation_start = as.Date(paste0(Year,"-01-01")),
          date_aggregation_end = as.Date(paste0(Year,"-12-31"))
        )
    }
    #For monthly aggregation
    if ( AggregationPeriods == "monthly" ) {
      AggregationPeriodsTmp <- tmp %>%
        mutate(
          nDaysPerMonth_end = days_in_month(MonthInData),
          date_aggregation_start = MonthInData,
          date_aggregation_end = as.Date(paste0(strftime(x=MonthInData,format="%Y-%m"),"-",nDaysPerMonth_end))
        ) 
    }
    AggregationPeriods <- AggregationPeriodsTmp %>%
      select(ID,date_aggregation_start,date_aggregation_end) %>%
      distinct()
  }
  
  

  #Basic idea: Repeat the each row as many times as the corresponding period has duration_days.
  #This yields data on a daily level. Perform monthly or annual aggregation on this dataset.
  #Benefits:
  # 1. You can arbitrarily aggregate over time (years, month, bi-weekly).
  # 2. Priods reaching over new years day (in case of annual aggregation)
  #or over the beginning of a month (in case of monthly aggregation) are not problematic.
  # 3. Aggregation of parallel measurements from only partially overlapping periods
  #(e.g. slightly different date_start,date_end) is not problematic.
  
  
  #Actual aggregation function called per data subset (per ID)
  AggregationFun <- function(DataSubSetPerID,AggregationPeriods) {
    
    #To make the debugging easier, the actual aggregation computation is wrapped inside a
    #tryCatch() construct. This allows for example to report the ID of the data that caused
    #an error.
    
    #Start of try-block for tryCatch
    Result <- tryCatch({
    
      Sub <- DataSubSetPerID
      DataSubSetPerID$ID = as.character(DataSubSetPerID$ID)
      AggregationPeriods$ID = as.character(AggregationPeriods$ID)
      CurrentID <- unique(Sub$ID)
      
      if ( (length(CurrentID) != 1) ) stop("ID must be unique among data send to AggregationFun()")
      
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
      
      #Create daily means - avg over parallel measurements
      SubDailyMean <- aggregate(value ~ d_1900,data=SubDaily,FUN=mean)
      
      #Convert AggregationPeriods to days after 1900
      Aggs <- AggregationPeriods %>%
        filter(ID == CurrentID)
      if ( nrow(Aggs) == 0 ) return()
      Aggs$date_aggregation_start_d1900 <- as.numeric(difftime(time1=Aggs$date_aggregation_start,time2=as.Date("1900-01-01"),units="days"))
      Aggs$date_aggregation_end_d1900 <- as.numeric(difftime(time1=Aggs$date_aggregation_end,time2=as.Date("1900-01-01"),units="days"))
      Aggs$Mean = NA
      Aggs$Median = NA
      Aggs$Sum = NA
      Aggs$Min = NA
      Aggs$Max = NA
      Aggs$TempCover = NA
      
      #For each AggregationPeriod - aggregate!
      for ( iAP in 1:nrow(Aggs) ) {
        tmp <- SubDailyMean %>%
          filter(
            d_1900 >= Aggs$date_aggregation_start_d1900[iAP],
            d_1900 <= Aggs$date_aggregation_end_d1900[iAP]
          )
        if ( nrow(tmp) == 0 ) {
          Aggs$TempCover[iAP] <- 0
          next
        }
        Aggs$Mean[iAP] = mean(tmp$value)
        Aggs$Median[iAP] = median(tmp$value)
        Aggs$Sum[iAP] = sum(tmp$value)
        Aggs$Min[iAP] = min(tmp$value)
        Aggs$Max[iAP] = max(tmp$value)
        Aggs$TempCover[iAP] = nrow(tmp) / (Aggs$date_aggregation_end_d1900[iAP] - Aggs$date_aggregation_start_d1900[iAP] + 1)
      }
      
      #tryCatch()' will return the last evaluated expression 
      # in case the "try" part was completed successfully
      Aggs <- Aggs %>%
        select(-date_aggregation_start_d1900,-date_aggregation_end_d1900)
      
    }, #End of try-block for tryCatch
    
    #Action if the try-block resultated in an error:
    error = function(error_condition) {
      message(paste("Error in TemporalAggregation() function for data with ID:", CurrentID,". The error meassage was:"))
      message(error_condition)
      return(NA)
    },
    
    #Action if the try-block resultated in a warnings
    warning = function(warning_condition) {
      message(paste("Warning in TemporalAggregation() function for data with ID:", CurrentID,". The error meassage was:"))
      message(error_condition)
      return(NA)
    },
    
    #Code to be executed regardless of whether try-catch succeeded or failed
    finally = {}
    
  ) #End of the try-catch construct
    
  return(Result)
    
  } #end of function to aggregate per ID
  
  
  #Set up parallel processing
  nCores <- detectCores(all.tests = FALSE, logical = TRUE)
  nCoresUse <- nCores - 1
  MyCluster <- makeCluster(nCoresUse)
  #Make available libraries in clusters
  clusterEvalQ(
    cl=MyCluster,{
      library(tidyverse); #data manipulation
      require(Hmisc) #Provides number of days per month or year
      }
  )
  #Split the data into a list: All data rows of the same ID form one list element
  DataToAggregateAsList <- split(x=DataToAggregate,f=DataToAggregate$ID)
  #Calculate annual aggregates using parallel processing
  AggsList <- parLapply(cl=MyCluster, X=DataToAggregateAsList,fun=AggregationFun,AggregationPeriods=AggregationPeriods)
  #Stop the parallel processing cluster
  stopCluster(MyCluster)
  #Row-Bind the resulting list into a dataframe
  AggsDF <- do.call(bind_rows, AggsList)
  #Drop row names
  row.names(AggsDF) <- NULL
  #Calculate AggregationPeriodLength_days
  #and round TempCover
  AggsDF <- AggsDF %>%
    mutate(
      TempCover = round(TempCover,2),
      AggregationPeriodLength_days = as.numeric(difftime(
        time1 = date_aggregation_end,
        time2 = date_aggregation_start,
        units = "days"
      )) + 1 #Plus one because both the first and last day are counted
    )
  #Reorder columns
  Cols <- colnames(AggsDF)
  FirstCols <- c("ID","date_aggregation_start","date_aggregation_end","AggregationPeriodLength_days","TempCover")
  OtherCols <- Cols[!(Cols %in% FirstCols)]
  AggsDF <- AggsDF[,c(FirstCols,OtherCols)]
  #Return
  return(AggsDF)
}




