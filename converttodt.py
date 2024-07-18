from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp

spark = SparkSession.builder.getOrCreate()

# Replace with your actual GCS bucket and file paths
source_bucket = "testdata7"
source_file = "202402/allstate-test_20240213_00e2ca13-03f0-4623-aae0-bc8801b9f573_20240213030905.orc"
destination_bucket = "your-destination-bucket"
destination_path = "202402/modified/allstate-test_20240213_00e2ca13-03f0-4623-aae0-bc8801b9f573_20240213030905.orc"

# String column name and its datetime format
string_column_name = "tripStartTimestamp" 
datetime_format = "yyyy-MM-dd'T'HH:mm:ssXXX"  # Adjust format as needed

# Read the ORC file from the source bucket
#df = spark.read.orc(f"gs://{source_bucket}/{source_file}")
df = spark.read.orc(f"gs://testdata7/bqtest/202403/allstate-test_20240213_0a736720-df16-4268-b592-931016791fda_20240213071659.orc")

# Convert the string column to datetime
df = df.withColumn(string_column_name, to_timestamp(string_column_name, datetime_format))

# Write the modified DataFrame to the destination bucket as ORC
df.write.mode("overwrite").orc(f"gs://testdata7/bqtest/202403/modified/")
