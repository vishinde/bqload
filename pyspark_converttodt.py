from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp, add_months, col

spark = SparkSession.builder.getOrCreate()

# Replace with your actual GCS bucket and file paths
source_bucket = "testdata7"
destination_bucket = "your-destination-bucket"
monthly_folders = ["202403","202404"]

for folder in monthly_folders:
    source_file = f"gs://testdata7/bqtest/{folder}/*.orc"
    destination_path = f"gs://testdata7/bqtest/{folder}/modified/"

    # String column name and its datetime format
    string_column_name = "tripStartTimestamp" 
    datetime_format = "yyyy-MM-dd'T'HH:mm:ssXXX"  # Adjust format as needed

    # Read the ORC file from the source bucket
    #df = spark.read.orc(f"gs://{source_bucket}/{source_file}")
    df = spark.read.orc(source_file)

    # Convert the string column to datetime
    df = df.withColumn(string_column_name, to_timestamp(string_column_name, datetime_format))
#    df = df.withColumn(string_column_name, add_months(col(string_column_name), 1))
#    df = df.withColumn(string_column_name, to_timestamp(string_column_name, datetime_format))

    # Write the modified DataFrame to the destination bucket as ORC
    df.write.mode("overwrite").orc(destination_path)
