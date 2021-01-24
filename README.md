R function to aggregate period-wise data over time.

 - See TemporalAggregation_Demo.R for an example
 
 - The script can aggregate per month, per year or for a user-defined time-period (e.g. growing season only).
 
 - In a first step, parallel measurements are averaged on a daily basis. This allows averaging over parallel measurements that only partially overlap in time.
 
 - In a second step, the resulting daily values are aggregated over time (month, year, or other periods). This means that the aggregates (e.g. Mean, Max) are not calculated from original measurement values but from daily averages over parallel measurements (if any parallel measurements are present).
 
 - The following aggregates are reported: Mean, Median, Min, Max, Sum and proportion of time covered by measurements compared to the duration of the aggregation period.

 - Input data must consist of columns "ID", "date_start", "date_end", "value" (see DummyData.csv). "ID" identifies the groups of data for averaging over parallel measurements (first step) and aggregation (second step).
 
 - Note that values in the column "value" of the input data are interpreted as daily values (e.g. average concentration in the period or average daily element flux in the period). The "Sum" column of the aggregated output reports the integral of the daily values (averaged over parallel measurements) over the aggregation period. In order to aggregate data that describes the total element flux in the period, convert to daily element fluxes first (divide by period duration).
 
 - Note that both date_start and date_end are interpreted to be completely included in the respective measurement period. Thus, if subsequent measurements did not overlap in time, make sure that date_end does not overlap with date_start of the next period. Function "HarmonizeDateEndCoding.R" can be used to adjust the end dates of periods if necessary.
 
Correct in case of non-overlapping sampling:

|ID| date_start  | date_end |
|--| ------------- | ------------- |
|A| 2020-03-01  | 2020-03-31  |
|A| 2020-04-01  | 2020-04-30  |

Wrong in case of non-overlapping sampling (will lead to an understimation of the aggregation output ("Sum") because one day of the two periods is averaged before summation (treated as parallel measurement)):

|ID| date_start  | date_end |
|--| ------------- | ------------- |
|A| 2020-03-01  | 2020-03-31  |
|A| 2020-03-31  | 2020-04-30  |


- Requires R packages tidyverse, parallel, lubridate and Hmisc.
