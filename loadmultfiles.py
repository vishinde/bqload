from google.cloud import bigquery

client = bigquery.Client()

monthly_folders = ["202403","202404"]

for folder in monthly_folders:
    uri = f"gs://testdata7/bqtest/{folder}/modified/*.orc"

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.ORC,
        time_partitioning=bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.MONTH,
            field="tripStartTimestamp",  # Specify the partitioning field
        ),
    )

    load_job = client.load_table_from_uri(
        uri,
        "loadbq.newparttbl",
        job_config=job_config,
    )

    load_job.result() 