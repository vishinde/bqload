bq load \
--source_format=ORC \
--time_partitioning_field=tripStartTimestamp --time_partitioning_type=MONTH \
loadbq.newparttbl \
gs://testdata7/bqtest/202401/modified/*.orc
gs://testdata7/bqtest/202402/modified/*.orc
gs://testdata7/bqtest/202403/modified/*.orc

LOAD DATA INTO loadbq.newparttbl
#PARTITION BY DATE_TRUNC(PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S.%f',tripStartTimestamp), MONTH)
PARTITION BY DATE_TRUNC(tripStartTimestamp, MONTH)
FROM FILES (
  format = 'ORC',
  uris = ['gs://testdata7/bqtest/202403/modified/foldername/*.orc'] 
);

//You will have to write the above stmt multiple times
