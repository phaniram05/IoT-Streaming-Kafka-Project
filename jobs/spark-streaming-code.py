from pyspark import SparkConf
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructField, StructType, StringType, TimestampType, DoubleType, IntegerType
from pyspark.sql.functions import from_json, col, column

from config import configuration


def main():
    # https://mvnrepository.com/artifact/org.apache.spark/spark-sql-kafka-0-10_2.13/3.5.1
    # https://mvnrepository.com/artifact/org.apache.hadoop/hadoop-aws/3.3.1
    # https://mvnrepository.com/artifact/com.amazonaws/aws-java-sdk/1.12.665

    spark = SparkSession.builder \
        .appName("smart-city-kafka-streaming") \
        .config("spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
                "org.apache.hadoop:hadoop-aws:3.3.1,"
                "com.amazonaws:aws-java-sdk:1.11.469") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.access.key", configuration.get('AWS_ACCESS_KEY')) \
        .config("spark.hadoop.fs.s3a.secret.key", configuration.get('AWS_SECRET_KEY')) \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .getOrCreate()

    # Adjust the log level to ERROR
    spark.sparkContext.setLogLevel("ERROR")

    # topic schemas

    vehicle_schema = StructType([
        StructField("id", StringType()),
        StructField("device_id", StringType()),
        StructField("time_stamp", TimestampType()),
        StructField("location", StringType()),
        StructField("speed", DoubleType()),
        StructField("direction", StringType()),
        StructField("make", StringType()),
        StructField("model", StringType()),
        StructField("year", IntegerType()),
        StructField("fuel_type", StringType())
    ])

    gps_schema = StructType([
        StructField("id", StringType()),
        StructField("device_id", StringType()),
        StructField("time_stamp", TimestampType()),
        StructField("speed", DoubleType()),
        StructField("direction", StringType()),
        StructField("fuel_type", StringType())
    ])

    traffic_cam_schema = StructType([
        StructField("id", StringType()),
        StructField("device_id", StringType()),
        StructField("camera_id", StringType()),
        StructField("location", StringType()),
        StructField("time_stamp", TimestampType()),
        StructField("snapshot", StringType())
    ])

    weather_schema = StructType([
        StructField("id", StringType()),
        StructField("device_id", StringType()),
        StructField("time_stamp", TimestampType()),
        StructField("location", StringType()),
        StructField("temperature", IntegerType()),
        StructField("weather_condition", StringType()),
        StructField("precipitation", DoubleType()),
        StructField("wind_speed", DoubleType()),
        StructField("humidity", IntegerType()),
        StructField("air_quality_index", DoubleType())
    ])

    emergency_incident_schema = StructType([
        StructField("id", StringType()),
        StructField("device_id", StringType()),
        StructField("incident_id", IntegerType()),
        StructField("location", StringType()),
        StructField("time_stamp", TimestampType()),
        StructField("type", StringType()),
        StructField("incident_status", IntegerType()),
        StructField("incident_desc", IntegerType())
    ])

    def read_kafka_topic(topic, schema):
        return (spark.readStream
                .format("kafka")
                .option("kafka.bootstrap.servers", "broker:29092")
                .option("subscribe", topic)
                .option("startingOffsets", "earliest")
                .load()
                .selectExpr('CAST(value AS STRING)')
                .select(from_json(col('value'), schema).alias('data'))
                .select('data.*')
                .withWatermark('time_stamp', '2 minutes')
                )

    def stream_writer(input_df, checkpoint_folder, output):
        return (input_df.writeStream
                .format('parquet')
                .option("checkpointLocation", checkpoint_folder)
                .option("path", output)
                .outputMode("append")
                .start()
                )

    # Reading the data using spark readStream
    vehicle_df = read_kafka_topic('vehicle_data', vehicle_schema).alias('vehicle')
    gps_df = read_kafka_topic('gps_data', gps_schema).alias('gps')
    traffic_df = read_kafka_topic('traffic_data', traffic_cam_schema).alias('traffic')
    weather_df = read_kafka_topic('weather_data', weather_schema).alias('weather')
    emergency_df = read_kafka_topic('emergency_data', emergency_incident_schema).alias('emergency')

    # Join all the dataframes based on 'timestamp'

    query1 = stream_writer(vehicle_df, 's3a://smart-city-iot-kafka-project/checkpoints/vehicle_data',
                           's3a://smart-city-iot-kafka-project/data/vehicle_data')
    query2 = stream_writer(gps_df, 's3a://smart-city-iot-kafka-project/checkpoints/gps_data',
                           's3a://smart-city-iot-kafka-project/data/gps_data')
    query3 = stream_writer(traffic_df, 's3a://smart-city-iot-kafka-project/checkpoints/traffic_data',
                           's3a://smart-city-iot-kafka-project/data/traffic_data')
    query4 = stream_writer(weather_df, 's3a://smart-city-iot-kafka-project/checkpoints/weather_data',
                           's3a://smart-city-iot-kafka-project/data/weather_data')
    query5 = stream_writer(emergency_df, 's3a://smart-city-iot-kafka-project/checkpoints/emergency_data',
                           's3a://smart-city-iot-kafka-project/data/emergency_data')

    query5.awaitTermination()


if __name__ == '__main__':
    main()
