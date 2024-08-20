#SAMPLE SCRIPT:

#STEP 1: Load ORC data files in BQ Tables as is - FREE.
#Create separate 20 TB tables to make scanning cheaper while Inserting
#e.g. 1st table 20 TB
bq load \
--source_format=ORC \
loadbq.newparttbl_1 \
gs://testdata7/bqtest/200801/modified/*.orc \
gs://testdata7/bqtest/200802/modified/*.orc \
gs://testdata7/bqtest/200803/modified/*.orc

#2nd table 20 TB
bq load \
--source_format=ORC \
loadbq.newparttbl_2 \
gs://testdata7/bqtest/200901/modified/*.orc \
gs://testdata7/bqtest/200902/modified/*.orc \
gs://testdata7/bqtest/200903/modified/*.orc

#Step 2: Load data in daily partitioned table with CASTING:
#1. CTAS for loading 1st 20 TB table
CREATE TABLE loadbq.ptb1 (id INT64, pts DATETIME)
PARTITION BY DATE_TRUNC(pts, DAY) AS (
SELECT id, CAST(ts AS DATETIME) AS pts FROM loadbq.newparttbl_1
);

#2. Multiple Insert stmts with CAST to load rest 20 TB tables..
INSERT INTO loadbq.ptb1 
SELECT id, CAST(ts AS DATETIME) as pts
FROM loadbq.newparttbl_2

INSERT INTO loadbq.ptb1 
SELECT id, CAST(ts AS DATETIME) as pts
FROM loadbq.newparttbl_3
