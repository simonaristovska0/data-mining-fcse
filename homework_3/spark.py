import findspark

import json
import joblib
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import from_json, to_json, struct
findspark.init()
from pyspark.sql.types import StructType, StructField, DoubleType


spark = SparkSession.builder \
    .appName("lab3") \
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.0"
    ) \
    .getOrCreate()

dt_model = joblib.load("model/best_diabetes_model.pkl")


df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "health_data") \
    .option("startingOffsets", "latest") \
    .load()


json_structure = StructType([
    StructField("HighBP", DoubleType()),
    StructField("HighChol", DoubleType()),
    StructField("CholCheck", DoubleType()),
    StructField("BMI", DoubleType()),
    StructField("Smoker", DoubleType()),
    StructField("Stroke", DoubleType()),
    StructField("HeartDiseaseorAttack", DoubleType()),
    StructField("PhysActivity", DoubleType()),
    StructField("Fruits", DoubleType()),
    StructField("Veggies", DoubleType()),
    StructField("HvyAlcoholConsump", DoubleType()),
    StructField("AnyHealthcare", DoubleType()),
    StructField("NoDocbcCost", DoubleType()),
    StructField("GenHlth", DoubleType()),
    StructField("MentHlth", DoubleType()),
    StructField("PhysHlth", DoubleType()),
    StructField("DiffWalk", DoubleType()),
    StructField("Sex", DoubleType()),
    StructField("Age", DoubleType()),
    StructField("Education", DoubleType()),
    StructField("Income", DoubleType())
])





parsed_df = df \
    .withColumn("json_str", F.col("value").cast("string")) \
    .select(from_json("json_str", json_structure).alias("data")) \
    .select("data.*")


def predict_and_write(batch_df, batch_id):
    if not batch_df.isEmpty():

        pandas_df = batch_df.toPandas()

        predictions = dt_model.predict(pandas_df)

        pandas_df["diabetes_pred"] = predictions.astype(int)

        spark_df = spark.createDataFrame(pandas_df)

        kafka_df = spark_df.select(
            to_json(struct(*spark_df.columns)).alias("value")
        )

        kafka_df.show(truncate=False)

        kafka_df.write \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("topic", "health_data_predicted") \
            .save()



streaming_query = (
    parsed_df
        .writeStream
        .foreachBatch(predict_and_write)
        .outputMode("append")
        .start()
)

streaming_query.awaitTermination()


