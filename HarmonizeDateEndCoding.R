HarmonizeDateEndCoding <- function(Data,IDCols) {
  require(tidyverse)
 
  if ( !("date_end" %in% colnames(Data) ) ) {
    stop("Column date_end not present.")
  }
  if ( !("date_start" %in% colnames(Data) ) ) {
    stop("Column date_start not present.")
  }
  if ( !(class(Data$date_end) == "Date") ) {
    stop("Column date_end not of class Date")
  }  
  
  ColNamesKeep <- colnames(Data)
  Data$DateStartID <- apply(X = Data[,c(IDCols,"date_start")], MARGIN = 1, FUN = paste, collapse = "-")
  Data$DateEndID <- apply(X = Data[,c(IDCols,"date_end")], MARGIN = 1, FUN = paste, collapse = "-")
  Data <- Data %>%
    mutate(
      #Create a new column indicating for each row whether its "DateEndID" is equal
      #to another row's "DateStartID". If such a row exists, this means that a directly
      #subsequent period for the respective plot-sampler exists.
      AnotherPeriodStartingOnEndDayExists <- DateEndID %in% DateStartID,
      date_end = case_when(
        AnotherPeriodStartingOnEndDayExists == TRUE ~ date_end - 1,
        AnotherPeriodStartingOnEndDayExists == FALSE ~ date_end
      )
    )

  #Drop all auxilliary columns added in the previous steps, no longer necessary
  Data <- Data[,ColNamesKeep]    
  
  return(Data)
}

