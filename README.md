R function to aggregate period-wise data over time. For example, can calculate the average value per year.

 - See TemporalAggregation_Demo.R for an example
 
 - The script can aggregate per month, per year or for a user-defined time-period (e.g. growing season only).
 
 - In a first step, parallel measurements are averaged on a daily basis. This allows averaging over  parallel measurements that only partially overlap in time, which is sometimes the case in the ICPF-DB.
 
 - In a second step, the resulting daily values are aggregated over time (month, year, or other periods). This means that aggregates (e.g. max(), min(), ...) are not calculated from original measurement values but from daily averages over parallel measurements (if any parallel measurements are present).
 
 - The following aggregates are reported: Mean, Median, Min, Max, Sum and proportion of time covered by measurements compared to the duration of the aggregation period (month, year, ...).

 - Requires R packages tidyverse, parallel, lubridate and Hmisc.

 - Parameter "FixDateStartEndOverlap" (enabled by default):
Example:
ID   date_start   date_end
A    2020-03-01   2020-03-31
A    2020-03-31   2020-04-30
FixDateStartEndOverlap=TRUE will set date_end of first period from 2020-03-31 to 2020-03-30 before aggretation. I.e. it will reduce all date_end dates by one day
if an identical date_start date exists for the same ID. If FixDateStartEndOverlap=FALSE, dates are interpreted as provided (not changed). This means that values for the day(s) of overlap will be averaged. This means for example, that the "Sum" output of the aggregation procedure is smaller than manually adding up the values of the different periods without taking into account the overlap.
