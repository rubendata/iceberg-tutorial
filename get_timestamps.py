import boto3

# Initialize the S3 client
s3 = boto3.client('s3')

# Define the S3 bucket and folder (prefix) you want to work with
bucket_name = 'iceberg-tutorial-bucket-ruben'
folder_prefix = 'nyc_taxi_iceberg_data_manipulation/'

# List objects in the specified folder
objects = s3.list_objects_v2(Bucket=bucket_name, Prefix=folder_prefix)

# Create a list to store object information
object_info_list = []

# Create a set to store distinct timestamps
distinct_timestamps = set()

# Loop through each object and retrieve the timestamp
for obj in objects.get('Contents', []):
    object_key = obj['Key']
    
    # Skip if it's a directory (prefix) or not a data file
    if obj['Size'] == 0 or not object_key.endswith('.parquet'):
        continue

    # Use the head-object operation to get metadata for the file
    metadata = s3.head_object(Bucket=bucket_name, Key=object_key)

    # Extract the timestamp from the metadata
    timestamp = metadata['LastModified']
    
    # Add the object information (key and timestamp) to the list
    object_info_list.append((object_key, timestamp))

    # Add the timestamp to the set to ensure uniqueness
    distinct_timestamps.add(timestamp)

# Sort the list by timestamp in descending order
sorted_object_info = sorted(object_info_list, key=lambda x: x[1], reverse=True)

# Print the sorted list and distinct timestamps
print("Distinct Timestamps:")
for timestamp in distinct_timestamps:
    print(f"- {timestamp}")

# print("\nSorted Object List:")
# for object_info in sorted_object_info:
#     object_key, timestamp = object_info
#     print(f"Object Key: {object_key}, Timestamp: {timestamp}")
