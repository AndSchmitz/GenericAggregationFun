
#init-----
rm(list=ls())
graphics.off()
options(warnPartialMatchDollar = T)

library(tidyverse)

WorkDir <- "W:\\Tools und Skripte\\R Function CalculateMonthlyOrAnnualAggs"

source(file.path(WorkDir,"CalculateMonthlyOrAnnualAggs.R"))

#Prepare I/O
InDir <- file.path(WorkDir,"Input")
OutDir <- file.path(WorkDir,"Output")
dir.create(OutDir,showWarnings = F)

#Load dummy data
Dat <- read.table(file=file.path(InDir,"DummyData.csv"),sep=";",header = T)
#Show dummy data structure
str(Dat)

#Generate an ID per subset of data that should be aggregated. E.g. per country-plot-sampler_code
IDCols <- c("code_country","code_plot","code_sampler")
Dat$ID <- as.factor(apply(X=Dat[,IDCols],MARGIN=1,FUN=paste,collapse="-"))

#Calculate the duration of each measurment period
Dat$date_start <- as.Date(Dat$date_start,format = "%d.%m.%Y")
Dat$date_end <- as.Date(Dat$date_end,format = "%d.%m.%Y")
Dat$duration_days <- as.numeric(Dat$date_end - Dat$date_start)
#Discard periods with unexpected duration
Dat <- Dat[Dat$duration_days > 0,]

#Call the CalculateMonthlyOrAnnualAggs() function for each ID (data-subset) - calculate aggregates
#In this example, monthly aggregation is used.
#Parameter AggregationTimeSpan can be changed to "annual" for annual aggregates.
AggsList <- by(data=Dat, INDICES = Dat[,"ID"], FUN=CalculateMonthlyAndAnnualAggs, AggregationTimeSpan="monthly")
#Convert aggregates from a list to a dataframe
AggsDF <- do.call(rbind.data.frame, AggsList)
str(AggsDF)

#Merge information on ID columsn and save data
AggsDF <- merge(AggsDF,unique(Dat[,c(IDCols,"ID")]),all.x = T)
write.table(x=AggsDF,file = file.path(OutDir,"AggregatedData.csv"),sep=";",row.names = F)

#Plot results
AggsDF$Time <- AggsDF$year + AggsDF$month/12

#Monthly sum
ggplot(data=AggsDF,mapping = aes(x=Time,y=MonthlyAvg,fill=ID)) +
  geom_bar(stat="identity") +
  facet_wrap( ~ ID,scales = "free") +
  ylab("Monthly sum")
ggsave(filename = file.path(OutDir,"MonthlySum.png"),width=40,height = 15,units = "cm")

#Monthly temp cover
ggplot(data=AggsDF,mapping = aes(x=Time,y=PropTempCover,fill=ID)) +
  geom_bar(stat="identity") +
  facet_wrap( ~ ID,scales = "free") +
  ylab("Temporal coverage of measurements (proportion of month)")
ggsave(filename = file.path(OutDir,"MonthlyTempCover.png"),width=40,height = 15,units = "cm")


  