from pyspark.sql import SparkSession
from config.spark_config import SparkConfig
from pyspark.sql.types import *
from utils.logger import logger
from pyspark.sql.functions import *

def main():
    spark = SparkConfig.create_spark(logger=logger)

    vehicleSchema = StructType([
        StructField(name="id", dataType=StringType(), nullable=True),
        StructField(name="deviceId", dataType=StringType(), nullable=True),
        StructField(name="timestamp", dataType=TimestampType(), nullable=True),
        StructField(name="location", dataType=StringType(), nullable=True),
        StructField(name="speed", dataType=DoubleType(), nullable=True),
        StructField(name="direction", dataType=StringType(), nullable=True),
        StructField(name="make", dataType=StringType(), nullable=True),
        StructField(name="year", dataType=IntegerType(), nullable=True),
        StructField(name="fuelType", dataType=StringType(), nullable=True)
    ])

    gpsSchema = StructType([
        StructField(name="id", dataType=StringType(), nullable=True),
        StructField(name="deviceId", dataType=StringType(), nullable=True),
        StructField(name="timestamp", dataType=TimestampType(), nullable=True),
        StructField(name="speed", dataType=DoubleType(), nullable=True),
        StructField(name="direction", dataType=StringType(), nullable=True),
        StructField(name="vehicleType", dataType=StringType(), nullable=True)
    ])

    trafficCameraSchema = StructType([
        StructField(name="id", dataType=StringType(), nullable=True),
        StructField(name="deviceId", dataType=StringType(), nullable=True),
        StructField(name="timestamp", dataType=TimestampType(), nullable=True),
        StructField(name="camera_id", dataType=StringType(), nullable=True),
        StructField(name="location", dataType=StringType(), nullable=True),
        StructField(name="snapshot", dataType=StringType(), nullable=True)
    ])

    weatherSchema = StructType([
        StructField(name="id", dataType=StringType(), nullable=True),
        StructField(name="deviceId", dataType=StringType(), nullable=True),
        StructField(name="timestamp", dataType=TimestampType(), nullable=True),
        StructField(name="location", dataType=DoubleType(), nullable=True),
        StructField(name="temperature", dataType=DoubleType(), nullable=True),
        StructField(name="weatherCondition", dataType=StringType(), nullable=True),
        StructField(name="precipitation", dataType=DoubleType(), nullable=True),
        StructField(name="winSpeed", dataType=DoubleType(), nullable=True),
        StructField(name="humidity", dataType=DoubleType(), nullable=True),
        StructField(name="airQuantityIndex", dataType=DoubleType(), nullable=True)
    ])

    emergencyIncidentSchema = StructType([
        StructField(name="id", dataType=StringType(), nullable=True),
        StructField(name="deviceId", dataType=StringType(), nullable=True),
        StructField(name="timestamp", dataType=TimestampType(), nullable=True),
        StructField(name="incidentId", dataType=StringType(), nullable=True),
        StructField(name="type", dataType=StringType(), nullable=True),
        StructField(name="location", dataType=StringType(), nullable=True),
        StructField(name="status", dataType=StringType(), nullable=True),
        StructField(name="description", dataType=StringType(), nullable=True)
    ])

    def read_kafka_topic(topic, schema):
        """
        Read streaming data from Kafka topic.
        """
        return(spark.readStream
               .format("kafka")                                         # Read source from Kafka
               .option("kafka.bootstrap.servers", "broker:29092")      # Kafka broker address
               .option("subscribe", topic)                              # Kafka topic name
               .option("startingOffsets", "earliest")                   # Read old messages first
               .load()                                                  # Load stream
               .selectExpr("CAST(value as STRING)")                     # Convert binary value -> string
               .select(from_json(col("value"), schema).alias("data"))   # Parse JSON using schema
               .select("data.*")                                        # Flatten nested JSON
               .withWatermark("timestamp", "2 minutes")                 # Handle late-arriving data
               )

    def streamWriter(input: DataFrame, checkpointFolder, output):
        return (input.writeStream
                .format("parquet")
                .option("checkpointLocation", checkpointFolder)
                .option("path", output)
                .outputMode("append")
                .start()
        )

    vehicleDF = read_kafka_topic(topic="vehicle_data", schema=vehicleSchema).alias("vehicle")
    gpsDF = read_kafka_topic(topic="gps_data", schema=gpsSchema).alias("gps")
    trafficCameraDF = read_kafka_topic(topic="traffic_data", schema=trafficCameraSchema).alias("traffic")
    weatherDF = read_kafka_topic(topic="weather_data", schema=weatherSchema).alias("weather")
    emergencyDF = read_kafka_topic(topic="emergency_data", schema=emergencyIncidentSchema).alias("emergency")

    # Join all the DFs with id and timestamp
    vehicle_query = streamWriter(vehicleDF, checkpointFolder="s3a://02-smart-city/checkpoints/vehicle_data",
                                 output="s3a://02-smart-city/data/vehicle_data")
    gps_query = streamWriter(gpsDF, checkpointFolder="s3a://02-smart-city/checkpoints/gps_data",
                             output="s3a://02-smart-city/data/gps_data")
    traffic_query = streamWriter(trafficCameraDF, checkpointFolder="s3a://02-smart-city/checkpoints/traffic_data",
                                 output="s3a://02-smart-city/data/traffic_data")
    weather_query = streamWriter(weatherDF, checkpointFolder="s3a://02-smart-city/checkpoints/weather_data",
                                 output="s3a://02-smart-city/data/weather_data")
    emergency_query = streamWriter(emergencyDF, checkpointFolder="s3a://02-smart-city/checkpoints/emergency_data",
                                   output="s3a://02-smart-city/data/emergency_data")
    emergency_query.awaitTermination()

if __name__ == "__main__":
    main()
