R function to average over parallel measurements and aggregate measurements to a monthly or annual basis.
 - The following aggregates are reported: Mean, Median, Min, Max, Sum and proportion of time covered by measurements per month or year.
 - In a first step, parallel measurements are averaged on a daily basis. This allows averaging over  parallel measurements that only partially overlap in time, which is sometimes the case in the ICPF-DB.
 - In a second step, the resulting daily values are aggregated to monthly or annual level. I.e. in case of parallel measurements, aggregates (e.g. Max) are not calculated from original measurement values but from daily averages.
