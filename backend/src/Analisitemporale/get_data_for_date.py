from pyspark.sql import SparkSession
from pyspark.sql.functions import col, date_format
import math
import os


def get_data_for_day(input_path, output_path, target_data):
    chunk_size_percent=2
    # Create a Spark session with configurations for partitioning and memory usage
    spark = SparkSession.builder \
        .appName("BigDataProject") \
        .master("local[4]") \
        .config("spark.sql.shuffle.partitions", "100") \
        .config("spark.sql.files.maxPartitionBytes", "128MB") \
        .getOrCreate()

    # Get list of Parquet files in the input path
    try:
        input_files = [os.path.join(input_path, f) for f in os.listdir(input_path) if f.endswith('.parquet')]
    except FileNotFoundError:
        print(f"Error: Input path {input_path} not found.")
        spark.stop()
        return

    total_files = len(input_files)
    chunk_size = math.ceil(total_files * (chunk_size_percent / 100.0)) # Calculate chunk size

    print(f"Total files in dataset: {total_files}")
    print(f"Chunk size (percentage): {chunk_size}")

    # Calculate number of chunks
    num_chunks = math.ceil(total_files / chunk_size)
    print(f"Number of chunks to process: {num_chunks}")

    # Process data in chunks
    for i in range(num_chunks):
        # Extract file paths for the current chunk
        start_index = i * chunk_size
        end_index = min((i + 1) * chunk_size, total_files)
        chunk_files = input_files[start_index:end_index]
        print(f"Processing chunk {i+1}/{num_chunks}, files from {start_index} to {end_index}")

        try:
            # Read chunk files into a DataFrame
            df_chunk = spark.read.parquet(*chunk_files)

            # Apply transformations (filter for date and format created_at)
            df_transformed = df_chunk.withColumn("created_at_str", date_format(col("created_at"), "yyyy-MM-dd")) \
                .filter(col("created_at_str") == target_data)

            # Write transformed chunk DataFrame to Parquet
            df_transformed.write.parquet(f"{output_path}/chunk_{i+1}.parquet", mode="overwrite")
        except Exception as e:
            print(f"Error processing chunk {i+1}: {e}")
    # Carica il file Parquet
    parquet_files = [os.path.join(output_path, f) for f in os.listdir(output_path) if f.endswith(".parquet")]

    # Carica tutti i file in un unico DataFrame
    df = spark.read.parquet(*parquet_files)

    # Visualizza il DataFrame completo
    df.show()
    return df






