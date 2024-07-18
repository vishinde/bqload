bq load \
--source_format=ORC \
--time_partitioning_field=tripStartTimestamp --time_partitioning_type=MONTH \
loadbq.newparttbl \
gs://testdata7/bqtest/202401/modified/*.orc
gs://testdata7/bqtest/202402/modified/*.orc
gs://testdata7/bqtest/202403/modified/*.orc
