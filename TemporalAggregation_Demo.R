
#
#Initialization---------
#
rm(list=ls())
graphics.off()
options(warnPartialMatchDollar = T)

library(tidyverse)

#Adjust work dir path
WorkDir <- "C:\\Users\\aschmitz\\Desktop\\tmp\\R Function TemporalAggregation"

#Load the function that does the actual work
source(file.path(WorkDir,"TemporalAggregation.R"))

#Prepare I/O
InDir <- file.path(WorkDir,"Input")
OutDir <- file.path(WorkDir,"Output")
dir.create(OutDir,showWarnings = F)

#Load dummy data (coming from ICPF deposition data) 
Dat <- read.table(file=file.path(InDir,"DummyData.csv"),sep=";",header = T)
#Show dummy data structure
str(Dat)




#
#Preparations------
#

#Important:
#Generate an ID per subset of data that should be aggregated. E.g. per country-plot-sampler_code
IDCols <- c("code_country","code_plot","code_sampler")
Dat$ID <- as.factor(apply(X=Dat[,IDCols],MARGIN=1,FUN=paste,collapse="-"))

#Calculate the duration of each measurment period
Dat$date_start <- as.Date(Dat$date_start,format = "%d.%m.%Y")
Dat$date_end <- as.Date(Dat$date_end,format = "%d.%m.%Y")




#
#Aggregation------
#

#Three examples:

#1. Annual aggregation
AnnualAggs <- TemporalAggregation(DataToAggregate=Dat,AggregationPeriods="annual")

#2. Monthly aggregation
MonthlyAggs <- TemporalAggregation(DataToAggregate=Dat,AggregationPeriods="monthly")

#3. Growing season for code_sampler == 1 (throughfall) and whole-year for code_sampler == 2 (open field)
#Define desired aggregation periods
TF <- Dat %>%
  filter(code_sampler == 1) %>%
  mutate(
    date_aggregation_start = as.Date(paste0(survey_year,"-04-01")),
    date_aggregation_end = as.Date(paste0(survey_year,"-10-31"))
  ) %>%
  select(ID,date_aggregation_start,date_aggregation_end) %>%
  distinct()
OF <- Dat %>%
  filter(code_sampler == 2) %>%
  mutate(
    date_aggregation_start = as.Date(paste0(survey_year,"-01-01")),
    date_aggregation_end = as.Date(paste0(survey_year,"-12-31"))
  ) %>%
  select(ID,date_aggregation_start,date_aggregation_end) %>%
  distinct()
AggregationPeriods <- bind_rows(TF,OF)
#Aggregate 
CustomAggs <- TemporalAggregation(DataToAggregate=Dat,AggregationPeriods=AggregationPeriods)




#
#Plotting-----
#

AnnualAggs$Label = "annual"
MonthlyAggs$Label = "monthly"
CustomAggs$Label = "Custom"
PlotDat <- bind_rows(AnnualAggs,MonthlyAggs,CustomAggs)

ggplot(data=PlotDat,mapping = aes(x=date_aggregation_start,y=Median,color=Label,shape=Label)) +
  geom_point() +
  facet_wrap( ~ ID,scales="free_y")
ggsave(filename = file.path(OutDir,"Example_aggregation_results.png"),width=40,height = 15,units = "cm")
write.table(x=PlotDat,file = file.path(OutDir,"Example_aggregation_results_data.csv"),sep=";",row.names = F)





