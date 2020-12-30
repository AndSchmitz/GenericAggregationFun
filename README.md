R function to aggregate period-wise data over time. For example, can calculate the average value per year.

 - See TemporalAggregation_Demo.R for an example
 
 - The script can aggregate per month, per year or for a user-defined time-period (e.g. growing season only).
 
 - In a first step, parallel measurements are averaged on a daily basis. This allows averaging over  parallel measurements that only partially overlap in time, which is sometimes the case in the ICPF-DB.
 
 - In a second step, the resulting daily values are aggregated over time (month, year, or other periods). This means that aggregates (e.g. max(), min(), ...) are not calculated from original measurement values but from daily averages over parallel measurements (if any parallel measurements are present).
 
 - The following aggregates are reported: Mean, Median, Min, Max, Sum and proportion of time covered by measurements compared to the duration of the aggregation period (month, year, ...).

 - Requires R packages tidyverse, parallel, lubridate and Hmisc.
