import urllib.request
from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder.appName("WikimediaStreaming").getOrCreate()

# Define the schema for the incoming data
schema = "binary"

# URL of the Wikimedia stream
stream_url = "https://stream.wikimedia.org/v2/stream/recentchange"


# Function to fetch the stream data
def fetch_stream_data():
    response = urllib.request.urlopen(stream_url)
    for line in response:
        yield line.strip()


# Create a stream from the fetched data
dataStream = (
    spark.readStream.format("socket")
    .option("host", "localhost")
    .option("port", 9999)
    .load()
)

# Extract the data from the stream
extractedData = dataStream.select(dataStream.value.cast("string"))

# Start the streaming query
query = extractedData.writeStream.outputMode("append").format("console").start()

# Fetch and process the stream data
fetch_stream_data()

# Wait for the stream to finish
query.awaitTermination()
