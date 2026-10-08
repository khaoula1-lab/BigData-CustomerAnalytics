from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, when, lower, regexp_replace
from pyspark.sql.types import *
from pymongo import MongoClient

#shemas
CLICKSTREAM_SCHEMA = StructType([
    StructField("session_id", StringType(), True),
    StructField("user_id", IntegerType(), True),
    StructField("product_id", IntegerType(), True),
    StructField("product_title", StringType(), True),
    StructField("category", StringType(), True),
    StructField("price", FloatType(), True),
    StructField("rating", FloatType(), True),
    StructField("action", StringType(), True),
    StructField("page_visitee", StringType(), True),
    StructField("duree_visite", IntegerType(), True),
    StructField("source", StringType(), True),
    StructField("review_text", StringType(), True),
    StructField("timestamp", DoubleType(), True),
])

TRANSACTION_SCHEMA = StructType([
    StructField("session_id", StringType(), True),
    StructField("user_id", IntegerType(), True),
    StructField("product_id", IntegerType(), True),
    StructField("product_title", StringType(), True),
    StructField("category", StringType(), True),
    StructField("montant_paye", FloatType(), True),
    StructField("methode_paiement", StringType(), True),
    StructField("statut_paiement", StringType(), True),
    StructField("timestamp", DoubleType(), True),
])

SUPPORT_SCHEMA = StructType([
    StructField("session_id", StringType(), True),
    StructField("user_id", IntegerType(), True),
    StructField("motif_contact", StringType(), True),
    StructField("canal", StringType(), True),
    StructField("resolu", BooleanType(), True),
    StructField("timestamp", DoubleType(), True),
])

# mongodb connection

def get_db():
    client = MongoClient(
        "mongodb://localhost:27017,localhost:27018,localhost:27019/?replicaSet=rs0",
        serverSelectionTimeoutMS=5000
    )
    return client["ecommerce"]
# sauvegarde

def save_clicks_to_mongo(df, epoch_id):
    if df.isEmpty():
        return
    db = get_db()
    collection = db["clickstream"]
    rows = [row.asDict() for row in df.collect()]
    if rows:
        collection.insert_many(rows)
        print(f"[MongoDB] {len(rows)} clicks sauvegardés")

def save_transactions_to_mongo(df, epoch_id):
    if df.isEmpty():
        return
    db = get_db()
    collection = db["transactions"]
    rows = [row.asDict() for row in df.collect()]
    if rows:
        collection.insert_many(rows)
        print(f"[MongoDB] {len(rows)} transactions sauvegardées")

def save_support_to_mongo(df, epoch_id):
    if df.isEmpty():
        return
    db = get_db()
    collection = db["service_client"]
    rows = [row.asDict() for row in df.collect()]
    if rows:
        collection.insert_many(rows)
        print(f"[MongoDB] {len(rows)} tickets support sauvegardés")

# spark

spark = (
    SparkSession.builder
    .appName("Consumer_MongoDB")
    .master("local[2]")
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.0")
    .config("spark.sql.streaming.metricsEnabled", "false")
    .config("spark.sql.shuffle.partitions", "2")
    .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# kafka reader

def read_kafka(topic):
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("subscribe", topic)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

# stream preprocessing

clicks = (
    read_kafka("clickstream")
    .select(from_json(col("value").cast("string"), CLICKSTREAM_SCHEMA).alias("data"))
    .select("data.*")
    .withColumn("review_clean", lower(regexp_replace(col("review_text"), "[^a-zA-Z\\s]", "")))
    .withColumn(
        "action_score",
        when(col("action") == "purchase", 5)
        .when(col("action") == "add_to_cart", 3)
        .when(col("action") == "view", 1)
        .when(col("action") == "search", 1)
        .when(col("action") == "abandon", -1)
        .otherwise(0)
    )
    .withColumn(
        "recommendation",
        when(col("action") == "abandon", "Envoyer coupon -20%")
        .when(col("action") == "add_to_cart", "Livraison gratuite")
        .when(col("action") == "purchase", "Produits similaires")
        .otherwise("Continuer navigation")
    )
)

transactions = (
    read_kafka("transactions")
    .select(from_json(col("value").cast("string"), TRANSACTION_SCHEMA).alias("data"))
    .select("data.*")
)

support = (
    read_kafka("service_client")
    .select(from_json(col("value").cast("string"), SUPPORT_SCHEMA).alias("data"))
    .select("data.*")
)

# stream to mongodb
query_clicks = (
    clicks.writeStream
    .foreachBatch(save_clicks_to_mongo)
    .outputMode("append")
    .option("checkpointLocation", "/tmp/checkpoint_clicks")
    .trigger(processingTime="10 seconds")
    .start()
)

query_tx = (
    transactions.writeStream
    .foreachBatch(save_transactions_to_mongo)
    .outputMode("append")
    .option("checkpointLocation", "/tmp/checkpoint_tx")
    .trigger(processingTime="10 seconds")
    .start()
)

query_support = (
    support.writeStream
    .foreachBatch(save_support_to_mongo)
    .outputMode("append")
    .option("checkpointLocation", "/tmp/checkpoint_support")
    .trigger(processingTime="10 seconds")
    .start()
)

print("Consumer MongoDB démarré !")
print("Streams actifs : clickstream | transactions | service_client")

spark.streams.awaitAnyTermination()