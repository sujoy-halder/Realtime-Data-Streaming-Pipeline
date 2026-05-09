import logging

from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType
)

logging.basicConfig(level=logging.INFO)

# ------------------ Cassandra Setup ------------------

def create_keyspace(session):
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS spark_streams
        WITH replication = {
            'class': 'SimpleStrategy',
            'replication_factor': 1
        };
    """)


def create_table(session):
    session.execute("""
        CREATE TABLE IF NOT EXISTS spark_streams.created_users (
            id TEXT PRIMARY KEY,
            first_name TEXT,
            last_name TEXT,
            gender TEXT,
            address TEXT,
            post_code TEXT,
            email TEXT,
            username TEXT,
            registered_date TEXT,
            phone TEXT,
            picture TEXT
        );
    """)


def create_cassandra_connection():
    try:
        cluster = Cluster(['cassandra'])
        session = cluster.connect()

        logging.info("Connected to Cassandra successfully")

        return session

    except Exception as e:
        logging.error(f"Cassandra connection error: {e}")
        return None

# ------------------ Spark Setup ------------------

def create_spark_connection():

    try:

        spark = (
            SparkSession.builder
            .appName("SparkDataStreaming")

            # Optional if using standalone cluster
            .master("local[*]")

            .config(
                "spark.cassandra.connection.host",
                "cassandra"
            )

            .config(
                "spark.jars.packages",
                "com.datastax.spark:spark-cassandra-connector_2.12:3.4.1,"
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1"
            )

            .getOrCreate()
        )

        spark.sparkContext.setLogLevel("ERROR")

        logging.info("Spark connection created successfully")

        return spark

    except Exception as e:
        logging.error(f"Spark connection error: {e}")
        return None

# ------------------ Kafka Connection ------------------

def connect_to_kafka(spark):

    try:

        df = (
            spark.readStream
            .format("kafka")
            .option(
                "kafka.bootstrap.servers",
                "kafka:9092"
            )
            .option("subscribe", "users_created")
            .option("startingOffsets", "earliest")

            # Prevent failures from offset loss
            .option("failOnDataLoss", "false")

            .load()
        )

        logging.info("Connected to Kafka successfully")

        return df

    except Exception as e:
        logging.error(f"Kafka connection error: {e}")
        return None

# ------------------ Transform ------------------

def create_selection_df_from_kafka(df):

    schema = StructType([
        StructField("id", StringType(), True),
        StructField("first_name", StringType(), True),
        StructField("last_name", StringType(), True),
        StructField("gender", StringType(), True),
        StructField("address", StringType(), True),
        StructField("post_code", StringType(), True),
        StructField("email", StringType(), True),
        StructField("username", StringType(), True),
        StructField("registered_date", StringType(), True),
        StructField("phone", StringType(), True),
        StructField("picture", StringType(), True)
    ])

    return (
        df.selectExpr("CAST(value AS STRING)")
        .select(
            from_json(
                col("value"),
                schema
            ).alias("data")
        )
        .select("data.*")
    )

# ------------------ Main ------------------

if __name__ == "__main__":

    spark = create_spark_connection()

    if spark:

        kafka_df = connect_to_kafka(spark)

        if kafka_df is not None:

            final_df = create_selection_df_from_kafka(kafka_df)

            session = create_cassandra_connection()

            if session:

                create_keyspace(session)
                create_table(session)

                logging.info("Streaming started...")

                query = (
                    final_df.writeStream
                    .format("org.apache.spark.sql.cassandra")

                    # Persistent checkpoint location
                    .option(
                        "checkpointLocation",
                        "/opt/spark/checkpoints/users_created"
                    )

                    .option("keyspace", "spark_streams")
                    .option("table", "created_users")

                    .outputMode("append")

                    .start()
                )

                query.awaitTermination()